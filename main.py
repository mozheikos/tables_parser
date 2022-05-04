import sys
from typing import List

from db.connection import get_tidb_engine, get_result_db_engine, get_timeweb_engine, get_tip3_engine, \
    get_risk_score_engine
from db.models import Transaction, Account, Link, Tip3
from sqlalchemy import or_, and_, desc, update
from sqlalchemy.orm import Session
from base_sql import get_sql
from raw_wallet_sql import get_raw_sql
import datetime as dt

TIDB = get_tidb_engine()
TIMEWEB = get_timeweb_engine()
RS = get_risk_score_engine()
TIP3 = get_tip3_engine()
RESULT = get_result_db_engine()


def quote_for_query(s):
    return f'\'{s}\''


def compare():
    start = dt.datetime.now()
    print(start)
    sql_1 = get_raw_sql()
    session = Session(bind=RESULT)
    with TIMEWEB.connect() as conn:
        accounts = conn.execute(sql_1).fetchall()
    print(f"{len(accounts) = }")
    count = 1
    for account in accounts:
        wallet, type_, name = account

        stmt = update(Account).where(Account.address == wallet).values(type=type_, name=name). \
            execution_options(synchronize_session="fetch")

        result = session.execute(stmt)
        print(f"\r{count}", end='')
        count += 1
    session.close()
    print()
    print(dt.datetime.now() - start)


def get_tip3():
    print(f"start tip3 {dt.datetime.now()}")
    session = Session(bind=RESULT)

    offset = session.query(Tip3).count()

    with TIP3.connect() as conn:

        count_sql = f"""
            select count(*) from balances
        """

        count = int(conn.execute(count_sql).fetchone()[0])
        print(count)
        limit = 10000
        while offset < count:

            sql = f'''
                    select 
                        b.owner_address,
                        left(b.token, 250),
                        b.amount
                    from balances b limit {limit} offset {offset}
                '''
            tokens = conn.execute(sql).fetchall()
            for item in tokens:
                exist = session.query(Account).where(Account.address == owner).one_or_none()
                if exist:
                    owner, token, amount = item
                    new_tip3 = Tip3(
                        token=token,
                        amount=float(amount),
                        account_id=owner
                    )
                    session.add(new_tip3)
            session.commit()
            offset += limit
    session.close()
    print(f"end tip3 {dt.datetime.now()}")


def get_account(account_address: str) -> Account:
    workchain, address = account_address.split(':')

    account_dict = {}

    with TIDB.connect() as conn:
        sql = f'''
            select 
                CONCAT(`workchain`, _UTF8MB4':', LOWER(HEX(`address`))) as account_addr,
                balance / 1000000000 balance,
                CONCAT(`creator_wc`, _UTF8MB4':', LOWER(HEX(`creator_address`))) as creator_addr,
                created_at as deployed_at,
                updated_at
            from accounts a
            where `address` = unhex(\'{address}\') and workchain = {int(workchain)};
                    '''
        accounts_result = conn.execute(sql).fetchone()
        account_dict['address'], account_dict['balance'], account_dict['creator_addr'], account_dict['deployed_at'], \
            account_dict['updated_at'] = accounts_result

    with TIMEWEB.connect() as conn:
        sql = get_sql(account_address)
        type_name_result = conn.execute(sql).fetchone()
        if type_name_result:
            wallet, type_, name = type_name_result
        else:
            wallet, type_, name = (None, None, None)
        account_dict['type'] = type_ if type_ else ""
        account_dict['name'] = name if name else ""

    with RS.connect() as conn:
        sql = f"""
            select risk_score
            from wallets_walletscore ww
            where ww.wallet_id = '{account_address}'
        """
        rs_result = conn.execute(sql).fetchone()
        account_dict['risk_score'] = float(rs_result[0]) if rs_result else 0.0

    account_dict['deleted_at'] = dt.date.today()
    account_dict['created_at'] = dt.date.today()

    if account_dict['deployed_at']:
        deployed_at = dt.date.fromtimestamp(account_dict['deployed_at'])
    else:
        deployed_at = dt.date(1970, 1, 1)

    if account_dict['updated_at']:
        updated_at = dt.date.fromtimestamp(account_dict['updated_at'])
    else:
        updated_at = dt.date(1970, 1, 1)

    return Account(
        address=account_dict['address'],
        type=account_dict['type'],
        name=account_dict['name'],
        risk_score=account_dict['risk_score'],
        balance=account_dict['balance'],
        creator_addr=account_dict['creator_addr'] if account_dict['creator_addr'] else "N/A",
        deployed_at=deployed_at,
        updated_at=updated_at,
        deleted_at=account_dict['deleted_at'],
        created_at=account_dict['created_at'],
        loaded=True
    )


def get_transaction():
    print(f"start messages {dt.datetime.now()}")
    session = Session(bind=RESULT)
    offset = session.query(Transaction).count()

    with TIDB.connect() as conn:

        count_sql = f"""
            select count(*)
            from message_in_and_out_by_date miaobd
            where balance_delta != 0
        """

        count = conn.execute(count_sql).fetchone()[0]
        limit = 10000
        expression = f"order by `date`"
        while offset < count:

            sql = f"""
                select
                    IF (ISNULL(`in_message / src`), account_addr, `in_message / src`)  as source,
                    IF (ISNULL(`out_messages / 0 / dst`), account_addr , `out_messages / 0 / dst`) as destination,
                    `date` as created_at,
                    abs(balance_delta) as balance_delta
                    from message_in_and_out_by_date miaobd
                    where balance_delta != 0
                    {expression} limit {limit} offset {offset}
                """
            transactions = conn.execute(sql).fetchall()
            for message in transactions:
                keys = ('source', 'destination', 'created_at', 'balance_delta')
                transaction = dict(zip(keys, message))

                src = session.query(Account).where(
                    Account.address == transaction['source']
                ).one_or_none()
                if not src:
                    src = get_account(transaction['source'])
                    session.add(src)
                    session.commit()

                dst = session.query(Account).where(
                    Account.address == transaction['destination']
                ).one_or_none()
                if not dst:
                    dst = get_account(transaction['destination'])
                    session.add(dst)
                    session.commit()

                link = session.query(Link).where(and_(
                    Link.src_id == src.id, Link.dst_id == dst.id)
                ).one_or_none()
                if not link:
                    link = Link(
                        src_id=src.id,
                        dst_id=dst.id
                    )
                    session.add(link)
                    session.commit()

                new_transaction = Transaction(
                    balance_delta=transaction['balance_delta'],
                    created_At=transaction['created_at'],
                    link_id=link.id
                )
                session.add(new_transaction)
                session.commit()

            offset += limit
    session.close()
    print(f"end messages {dt.datetime.now()}")


if __name__ == '__main__':
    option = None
    try:
        option = sys.argv[1]
    except IndexError as e:
        print('invalid option. Choose: transactions, tip3 or accounts')
        exit(1)

    if option == 'transactions':
        get_transaction()
    elif option == 'tip3':
        get_tip3()
    elif option == 'accounts':
        compare()
    else:
        print('invalid option. Choose: transactions, tip3 or accounts')
        exit(1)
