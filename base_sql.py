def get_sql(argument: str) -> str:

    sql = f"""
            select 
            lower(res.wallet) as wallet,
            SUBSTRING_INDEX(GROUP_CONCAT(res.`type`), ',', 1) as `type`,
            SUBSTRING_INDEX(GROUP_CONCAT(res.`name`), ',', 1) as name 
            from
            (
            select distinct wallet, `type`, `name` 
            from (
                    (
                        select distinct
                        c.value as wallet, 'other' as `type`, u.login as `name`
                        from 
                        freeton.approved_contact_user acu
                        inner join 
                        freeton.contact c on c.id = acu.contact_id
                        left join 
                        freeton.`user` u on u.id = acu.user_id
                        where
                        c.type = 'wallet'
                    ) union
                    (
                        select  distinct 
                        wallet, `type`, name
                        from
                        freeton.wallets
                        where 
                        wallet is not null 
                    ) union 
                    (
                        select
                        wallet, 'other' as 'type', IF(not trim(forum_login) = '', forum_login, IF(not trim(forum_name)='',
                        forum_name, IF(not trim(tg_username) = '', CONCAT('tg: ', tg_username),
                        CONCAT('jury ', subgov)))) as 'name'
                        from
                        freeton.freeton_jury fj
                        where
                        wallet is not null
                    ) union 
                    (
                        select
                        multisig24Address as wallet, 'other' as type, img as name
                        from
                        blockchain.subgov
                        where 
                        trim(multisig24Address) <> ''
                    ) union 
                    (
                        select 
                        cr.address as wallet, 'other' as 'type', CONCAT('Contender ', s.img) as name
                        from
                        blockchain.contender cr
                        left join
                        blockchain.contest ct on ct.id = cr.contest_id
                        left join
                        blockchain.subgov s on s.id = ct.gov_id
                        where
                        cr.address <> '-1:7777777777777777777777777777777777777777777777777777777777777777'
                    ) union
                    (
                        select 
                        judge_address as wallet, 'other' as 'type', 'jury' as name
                        from
                        blockchain.proposal p
                    ) union 
                        (
                        select
                        wallet ,
                        'user' as 'type',
                        IF(not trim(tg_username) = '', CONCAT('tg: ', tg_username),
                        IF(not trim(full_name) = '', full_name,
                        IF(not trim(full_name_cyr) = '', full_name_cyr,
                        IF(not trim(email) = '', email,
                        IF(not trim(phone) = '', phone,
                        IF(not trim(facebook) = '', facebook,
                        IF(not trim(instagram) = '', instagram,
                        IF(not trim(vk) = '', vk,
                        IF(not trim(linkedin) = '', linkedin,
                        other))))))))) as 'name'
                        from
                        freeton.mint_user
                        WHERE 
                        TRIM(wallet) <> ''
                        )
                ) x
            where
            trim(name) <> ''
            and wallet = '{argument}'
            order by
            wallet, (x.`type`='other')
            ) res
            group by wallet   
            """
    return sql
