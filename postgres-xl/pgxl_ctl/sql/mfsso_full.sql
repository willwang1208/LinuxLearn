DROP ROLE mfsso;
CREATE ROLE mfsso LOGIN PASSWORD 'microfun_2016';

DROP DATABASE mfsso;
CREATE DATABASE mfsso OWNER mfsso ENCODING 'UTF8';

-- TOC entry 189 (class 1255 OID 32947)
-- Name: SODevLogin(int4, text, text, text, int4, text, text, text, text, text, text, timestamp without time zone); Type: FUNCTION; Schema: public; Owner: -
--
DROP FUNCTION public."SODevLogin"(integer, text, text, text, text, text, text, text, text, text, text, smallint, smallint, smallint);
CREATE or replace FUNCTION "SODevLogin"("appId" int4, 
                                        "deviceId" text, 
                                        "vendorId" text,
                                        "openUDID" text,
                                        "installationId" text,
                                        idfa text,
                                        imei text,
                                        imsi text,
                                        "wifiMAC" text,
                                        "bluetoothMAC" text,
                                        "socialId" text,
                                        "socialType" int2,
                                        --0:replace;1:new
                                        "policyType" int2,
                                        --0:ask;1:replace
                                        "bindFlag" int2)
 --rstatus: 0:new;1:old;2:merge;3;replace-success;4:replace-ask;5:error
 RETURNS TABLE("userId" int8, "registrationStatus" int2)
    LANGUAGE plpgsql ROWS 1
    AS $$
declare
    uid_dev int8;
    uid_social int8;
    uid int8;
    sid text;
    rstatus int2 ;
    "validKeyIn" text;
    "tempKey" text;
  
begin
    select 
        s."specialKey" into "tempKey"
    from 
        "SpecialKey" s
    where 
        s."specialKey" = "SODevLogin"."deviceId"
    ;
    if not found then
        "validKeyIn" = "SODevLogin"."deviceId";
    else
        select s."specialKey" into "tempKey"
        from 
            "SpecialKey" s
        where 
            s."specialKey" = "SODevLogin"."vendorId"
        ;
        if not found then
            "validKeyIn" = "SODevLogin"."vendorId";
        else
            select s."specialKey" into "tempKey"
            from "SpecialKey" s
            where s."specialKey" = "SODevLogin"."openUDID"
            ;
            if not found then
                "validKeyIn" = "SODevLogin"."openUDID";
            else
                select s."specialKey" into "tempKey"
                from "SpecialKey" s
                where s."specialKey" = "SODevLogin"."installationId"
                ;
                if not found then
                    "validKeyIn" = "SODevLogin"."installationId";
                else
                    "validKeyIn" = 'default';
                end if;
            end if;
        end if;
    end if;

    select cred."userId", cred."socialId" into uid_dev, sid
    from "DeviceIdentity" cred
    where 
        status = 0 
        and -- Non-zero status means disabled
        cred."validKey" = "validKeyIn"
        and
        cred."policyType" = "SODevLogin"."policyType"
    ;

    if "SODevLogin"."socialId" != '' then
        select cred."userId" into uid_social
        from "DeviceIdentity" cred
        where 
            status = 0 
            and -- Non-zero status means disabled
            cred."socialId" = "SODevLogin"."socialId"
            and
            cred."policyType" = "SODevLogin"."policyType"
        ;
    end if;

    if uid_dev is NULL and uid_social is NULL then
        --new
        rstatus = 0;
        uid = nextval('"User_id_Seq"');

        -- Insert into User table
        execute 'insert into "User" values ($1, '''', now(), 0)' using uid;

        insert into "DeviceIdentity"
        (
            "userId",
            "appId",
            "validKey",
            "deviceId",
            "vendorId",
            "openUDID",
            "installationId",
            "idfa", 
            "imei",
            "imsi",
            "wifiMAC",
            "bluetoothMAC",
            "socialId",
            "socialType",
            "policyType"   
        )
        values
        (
            uid,
            "SODevLogin"."appId",
            "validKeyIn",
            "SODevLogin"."deviceId",
            "SODevLogin"."vendorId",
            "SODevLogin"."openUDID",  
            "SODevLogin"."installationId",
            "SODevLogin"."idfa",   
            "SODevLogin"."imei",
            "SODevLogin"."imsi",
            "SODevLogin"."wifiMAC",
            "SODevLogin"."bluetoothMAC",
            "SODevLogin"."socialId",
            "SODevLogin"."socialType",
            "SODevLogin"."policyType"
        );

    elsif uid_dev is not NULL and uid_social is NULL then
        
        if "SODevLogin"."socialId" = '' then 
            --old		
            rstatus = 1;
            uid = uid_dev;
        elsif sid = '' then
            update "DeviceIdentity" d 
            set
                "socialId" = "SODevLogin"."socialId",
                "socialType" = "SODevLogin"."socialType",
                "policyType" = "SODevLogin"."policyType"
            where 
                d."userId" = uid_dev
            ;	
            --merge		
            rstatus = 2;
            uid = uid_dev;
        else
            if "SODevLogin"."policyType" = 0 then
                update "DeviceIdentity" d 
                set
                    "socialId" = "SODevLogin"."socialId",
                    "socialType" = "SODevLogin"."socialType",
                    "policyType" = "SODevLogin"."policyType"
                where 
                    d."userId" = uid_dev
                ;
                --replace-success			
                rstatus = 3;
                uid = uid_dev;
            elsif "SODevLogin"."policyType" = 1 then
                --new
                rstatus = 0;
                uid = nextval('"User_id_Seq"');

                -- Insert into User table
                execute 'insert into "User" values ($1, '''', now(), 0)' using uid;

                insert into "DeviceIdentity"
                (
                    "userId",
                    "appId",
                    "validKey",
                    "deviceId",
                    "vendorId",
                    "openUDID",
                    "installationId",
                    "idfa", 
                    "imei",
                    "imsi",
                    "wifiMAC",
                    "bluetoothMAC",
                    "socialId",
                    "socialType",
                    "policyType"   
                )
                values
                (
                    uid,
                    "SODevLogin"."appId",
                    "validKeyIn",
                    "SODevLogin"."deviceId",
                    "SODevLogin"."vendorId",
                    "SODevLogin"."openUDID",  
                    "SODevLogin"."installationId",
                    "SODevLogin"."idfa",   
                    "SODevLogin"."imei",
                    "SODevLogin"."imsi",
                    "SODevLogin"."wifiMAC",
                    "SODevLogin"."bluetoothMAC",
                    "SODevLogin"."socialId",
                    "SODevLogin"."socialType",
                    "SODevLogin"."policyType"
                );
            else
                --error
                rstatus = 5;
                uid = 0;
            end if;
        end if;

    -- uid_social is not NULL
    else 
        if uid_dev = uid_social then
            --old
            rstatus = 1;
            uid = uid_social;
        elsif "SODevLogin"."bindFlag" = 1 then 
            update "DeviceIdentity" d 
                set
                    "validKey" = "validKeyIn",
                    "deviceId" = "SODevLogin"."deviceId",
                    "vendorId" = "SODevLogin"."vendorId",
                    "openUDID" = "SODevLogin"."openUDID",
                    "installationId" = "SODevLogin"."installationId",
                    "idfa" = "SODevLogin"."idfa",
                    "imei" = "SODevLogin"."imei",
                    "imsi" = "SODevLogin"."imsi",
                    "wifiMAC" = "SODevLogin"."wifiMAC",
                    "bluetoothMAC" = "SODevLogin"."bluetoothMAC"
                where 
                    d."userId" = uid_social
                ;
                --replace-success			
                rstatus = 3;
                uid = uid_social;
        elsif "SODevLogin"."bindFlag" = 0 then 
            --replace-ask
            rstatus = 4;
            uid = uid_social;
        else
            --error
            rstatus = 5;
            uid = 0;
        end if;
    end if;

    return query select uid as "userId", rstatus as "registrationStatus";

end;

$$;

DROP FUNCTION public."SODevLogin"(integer, text, text, text, text, text, text, text, text, text);
CREATE or replace FUNCTION "SODevLogin"("appId" int4, 
                                        "deviceId" text, 
                                        "vendorId" text,
                                        "openUDID" text,
                                        "installationId" text,
                                        idfa text,
                                        imei text,
                                        imsi text,
                                        "wifiMAC" text,
                                        "bluetoothMAC" text)
 --rstatus: 0:new;1:old
 RETURNS TABLE("userId" int8, "registrationStatus" int2)
    LANGUAGE plpgsql ROWS 1
    AS $$
declare
    uid int8;
    rstatus int2 = 1;
    "validKeyIn" text;
    "tempKey" text;
  
begin
    select 
        s."specialKey" into "tempKey"
    from 
        "SpecialKey" s
    where 
        s."specialKey" = "SODevLogin"."deviceId"
    ;
    if not found then
        "validKeyIn" = "SODevLogin"."deviceId";
    else
        select s."specialKey" into "tempKey"
        from 
            "SpecialKey" s
        where 
            s."specialKey" = "SODevLogin"."vendorId"
        ;
        if not found then
            "validKeyIn" = "SODevLogin"."vendorId";
        else
            select s."specialKey" into "tempKey"
            from "SpecialKey" s
            where s."specialKey" = "SODevLogin"."openUDID"
            ;
            if not found then
                "validKeyIn" = "SODevLogin"."openUDID";
            else
                select s."specialKey" into "tempKey"
                from "SpecialKey" s
                where s."specialKey" = "SODevLogin"."installationId"
                ;
                if not found then
                    "validKeyIn" = "SODevLogin"."installationId";
                else
                    "validKeyIn" = 'default';
                end if;
            end if;
        end if;
    end if;

    select cred."userId" into uid
    from "DeviceIdentity" cred
    where 
        status = 0 
        and -- Non-zero status means disabled
        cred."validKey" = "validKeyIn"
    ;

    if not found then
        rstatus = 0 ;
        uid := nextval('"User_id_Seq"');

        -- Insert into User table
        execute 'insert into "User" values ($1, '''', now(), 0)' using uid;

        insert into "DeviceIdentity"
        (
            "userId",
            "appId",
            "validKey",
            "deviceId",
            "vendorId",
            "openUDID",
            "installationId",
            "idfa", 
            "imei",
            "imsi",
            "wifiMAC",
            "bluetoothMAC"
        )
        values
        (
            uid,
            "SODevLogin"."appId",
            "validKeyIn",
            "SODevLogin"."deviceId",
            "SODevLogin"."vendorId",
            "SODevLogin"."openUDID",  
            "SODevLogin"."installationId",
            "SODevLogin"."idfa",   
            "SODevLogin"."imei",
            "SODevLogin"."imsi",
            "SODevLogin"."wifiMAC",
            "SODevLogin"."bluetoothMAC"
        );
    end if;

    return query select uid as "userId", rstatus as "registrationStatus";

end;

$$;

CREATE or replace FUNCTION "SOAccountBind"("appId" int4, 
                                        "deviceId" text, 
                                        "vendorId" text,
                                        "openUDID" text,
                                        "installationId" text,
                                        idfa text,
                                        imei text,
                                        imsi text,
                                        "wifiMAC" text,
                                        "bluetoothMAC" text,
                                        "currentUserId" int8,
                                        "socialId" text,
                                        --0:device;1:facebook
                                        "socialType" int2,
                                        --0:append;1:new
                                        "policyType" int2,
                                        --0:not unbind device;1:unbind device
                                        "bindFlag" int2)
 --rstatus: 0:register and new userid;1:current userid;2: switch userid;3: bind and new userid;
 RETURNS TABLE("userId" int8, "registrationStatus" int2, "isDeviceUserId" int2)
    LANGUAGE plpgsql ROWS 1
    AS $$
declare
    uid int8;
    deviceuserid int8;
    rstatus int2 ;
    isdeviceuserid int2;
    "validKeyIn" text;
    "deviceKey" text;
    "tempKey" text;
begin
    select 
        s."specialKey" into "tempKey"
    from 
        "SpecialKey" s
    where 
        s."specialKey" = "SOAccountBind"."deviceId"
    ;
    if not found then
        "deviceKey" = "SOAccountBind"."deviceId";
    else
        select s."specialKey" into "tempKey"
        from 
            "SpecialKey" s
        where 
            s."specialKey" = "SOAccountBind"."vendorId"
        ;
        if not found then
            "deviceKey" = "SOAccountBind"."vendorId";
        else
            select s."specialKey" into "tempKey"
            from "SpecialKey" s
            where s."specialKey" = "SOAccountBind"."openUDID"
            ;
            if not found then
                "deviceKey" = "SOAccountBind"."openUDID";
            else
                select s."specialKey" into "tempKey"
                from "SpecialKey" s
                where s."specialKey" = "SOAccountBind"."installationId"
                ;
                if not found then
                    "deviceKey" = "SOAccountBind"."installationId";
                else
                    "deviceKey" = 'default';
                end if;
            end if;
        end if;
    end if;

    if "SOAccountBind"."socialId" != '' then
        "validKeyIn" = "SOAccountBind"."socialId";
    else
        "validKeyIn" = "deviceKey";
    end if;
        
    select 
        d."userId" into uid
    from 
        "DeviceIdentity" d
    where 
        d."validKey" = "validKeyIn"
    ;
    if found then
        rstatus = 1;
        if "SOAccountBind"."socialId" = '' then
            isdeviceuserid = 1;
        else
            select 
                d."userId" into deviceuserid
            from 
                "DeviceIdentity" d
            where 
                d."validKey" = "deviceKey"
            ;
        end if;
        if found then
            if deviceuserid = uid then
                isdeviceuserid = 1;
            else
                isdeviceuserid = 0;
            end if;
        else
            isdeviceuserid = 0;
        end if;
    else
        --new
        if "SOAccountBind"."policyType" = 1 then
            rstatus = 0;
            if "SOAccountBind"."socialId" = '' then
                isdeviceuserid = 1;
            else
                isdeviceuserid = 0;
            end if;
            uid = nextval('"User_id_Seq"');

            -- Insert into User table
            execute 'insert into "User" values ($1, '''', now(), 0)' using uid;

            insert into "DeviceIdentity"
            (
                "userId",
                "appId",
                "validKey",
                "deviceId",
                "vendorId",
                "openUDID",
                "installationId",
                "idfa", 
                "imei",
                "imsi",
                "wifiMAC",
                "bluetoothMAC",
                "socialId",
                "socialType",
                "policyType"
            )
            values
            (
                uid,
                "SOAccountBind"."appId",
                "validKeyIn",
                "SOAccountBind"."deviceId",
                "SOAccountBind"."vendorId",
                "SOAccountBind"."openUDID",  
                "SOAccountBind"."installationId",
                "SOAccountBind"."idfa",   
                "SOAccountBind"."imei",
                "SOAccountBind"."imsi",
                "SOAccountBind"."wifiMAC",
                "SOAccountBind"."bluetoothMAC",
                "SOAccountBind"."socialId",
                "SOAccountBind"."socialType",
                "SOAccountBind"."policyType"
            );
        --append
        elsif "SOAccountBind"."policyType" = 0 then
            if "SOAccountBind"."currentUserId" != 0 then
                uid = "SOAccountBind"."currentUserId";
            elsif "SOAccountBind"."socialId" != '' then
                select 
                    d."userId" into uid
                from 
                    "DeviceIdentity" d
                where 
                    d."validKey" = "deviceKey";
                if not found then
                    uid = 0;
                end if;
            else
                uid = 0;
            end if;
                
            --not register
            if uid = 0 then
                --not unbind device
                if "SOAccountBind"."bindFlag" = 0 then
                    rstatus = 0;
                    isdeviceuserid = 1;
                    uid = nextval('"User_id_Seq"');

                    -- Insert into User table
                    execute 'insert into "User" values ($1, '''', now(), 0)' using uid;

                    insert into "DeviceIdentity"
                    (
                        "userId",
                        "appId",
                        "validKey",
                        "deviceId",
                        "vendorId",
                        "openUDID",
                        "installationId",
                        "idfa", 
                        "imei",
                        "imsi",
                        "wifiMAC",
                        "bluetoothMAC",
                        "socialId",
                        "socialType",
                        "policyType"
                    )
                    values
                    (
                        uid,
                        "SOAccountBind"."appId",
                        "deviceKey",
                        "SOAccountBind"."deviceId",
                        "SOAccountBind"."vendorId",
                        "SOAccountBind"."openUDID",  
                        "SOAccountBind"."installationId",
                        "SOAccountBind"."idfa",   
                        "SOAccountBind"."imei",
                        "SOAccountBind"."imsi",
                        "SOAccountBind"."wifiMAC",
                        "SOAccountBind"."bluetoothMAC",
                        "SOAccountBind"."socialId",
                        0,
                        "SOAccountBind"."policyType"
                    );
                    if "SOAccountBind"."socialId" != '' then
                        insert into "DeviceIdentity"
                        (
                            "userId",
                            "appId",
                            "validKey",
                            "deviceId",
                            "vendorId",
                            "openUDID",
                            "installationId",
                            "idfa", 
                            "imei",
                            "imsi",
                            "wifiMAC",
                            "bluetoothMAC",
                            "socialId",
                            "socialType",
                            "policyType"
                        )
                        values
                        (
                            uid,
                            "SOAccountBind"."appId",
                            "SOAccountBind"."socialId",
                            "SOAccountBind"."deviceId",
                            "SOAccountBind"."vendorId",
                            "SOAccountBind"."openUDID",  
                            "SOAccountBind"."installationId",
                            "SOAccountBind"."idfa",   
                            "SOAccountBind"."imei",
                            "SOAccountBind"."imsi",
                            "SOAccountBind"."wifiMAC",
                            "SOAccountBind"."bluetoothMAC",
                            "SOAccountBind"."socialId",
                            "SOAccountBind"."socialType",
                            "SOAccountBind"."policyType"
                        );
                    end if;
                --unbind device
                elsif "SOAccountBind"."bindFlag" = 1 then
                    rstatus = 0;
                    isdeviceuserid = 0;
                    uid = nextval('"User_id_Seq"');

                    -- Insert into User table
                    execute 'insert into "User" values ($1, '''', now(), 0)' using uid;

                    insert into "DeviceIdentity"
                    (
                        "userId",
                        "appId",
                        "validKey",
                        "deviceId",
                        "vendorId",
                        "openUDID",
                        "installationId",
                        "idfa", 
                        "imei",
                        "imsi",
                        "wifiMAC",
                        "bluetoothMAC",
                        "socialId",
                        "socialType",
                        "policyType"
                    )
                    values
                    (
                        uid,
                        "SOAccountBind"."appId",
                        "validKeyIn",
                        "SOAccountBind"."deviceId",
                        "SOAccountBind"."vendorId",
                        "SOAccountBind"."openUDID",  
                        "SOAccountBind"."installationId",
                        "SOAccountBind"."idfa",   
                        "SOAccountBind"."imei",
                        "SOAccountBind"."imsi",
                        "SOAccountBind"."wifiMAC",
                        "SOAccountBind"."bluetoothMAC",
                        "SOAccountBind"."socialId",
                        "SOAccountBind"."socialType",
                        "SOAccountBind"."policyType"
                    );
                end if;
            --already registered
            else
                rstatus = 1;
                isdeviceuserid = 1;
                insert into "DeviceIdentity"
                    (
                        "userId",
                        "appId",
                        "validKey",
                        "deviceId",
                        "vendorId",
                        "openUDID",
                        "installationId",
                        "idfa", 
                        "imei",
                        "imsi",
                        "wifiMAC",
                        "bluetoothMAC",
                        "socialId",
                        "socialType",
                        "policyType"
                    )
                    values
                    (
                        uid,
                        "SOAccountBind"."appId",
                        "validKeyIn",
                        "SOAccountBind"."deviceId",
                        "SOAccountBind"."vendorId",
                        "SOAccountBind"."openUDID",  
                        "SOAccountBind"."installationId",
                        "SOAccountBind"."idfa",   
                        "SOAccountBind"."imei",
                        "SOAccountBind"."imsi",
                        "SOAccountBind"."wifiMAC",
                        "SOAccountBind"."bluetoothMAC",
                        "SOAccountBind"."socialId",
                        "SOAccountBind"."socialType",
                        "SOAccountBind"."policyType"
                    );

                --unbind device
                if "SOAccountBind"."bindFlag" = 1 then
                    isdeviceuserid = 0;
                    delete from "DeviceIdentity" d where d."userId" = uid and d."socialType" = 0;
                end if;
            end if;
        end if;
    end if;

    return query select uid "userId", rstatus "registrationStatus", isdeviceuserid "isDeviceUserId";  

end;

$$;

--
-- TOC entry 1897 (class 2606 OID 24629)
-- Name: User_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

--
-- TOC entry 175 (class 1259 OID 32938)
-- Name: DeviceIdentity; Type: TABLE; Schema: public; Owner: -
--
DROP TABLE public."DeviceIdentity";
CREATE TABLE "DeviceIdentity" (
    "userId" int8,
    "appId" int4,
    "validKey" text NOT NULL,
    "deviceId" text,
    "vendorId" text,
    "openUDID" text,   
    "installationId" text,
    idfa text,     
    imei text,
    imsi text,
    "wifiMAC" text,
    "bluetoothMAC" text,
    "socialId" text,
    --0:device; 1:facebook
    "socialType" int2,
    --0:append; 1:new
    "policyType" int2,  
    --0:valid; other:disable  
    status int2 DEFAULT 0,
    "createTime" timestamp without time zone DEFAULT now(),
    CONSTRAINT "PK_DeviceIdentity" PRIMARY KEY ("validKey")
) distribute by hash("validKey");

--
-- TOC entry 172 (class 1259 OID 24608)
-- Name: User_id_Seq; Type: SEQUENCE; Schema: public; Owner: -
--
DROP SEQUENCE public."User_id_Seq";
ALTER SEQUENCE "User_id_Seq" RESTART WITH 10000;
CREATE SEQUENCE "User_id_Seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- TOC entry 173 (class 1259 OID 24622)
-- Name: User; Type: TABLE; Schema: public; Owner: -
--
DROP TABLE public."User";
CREATE TABLE "User" (
    id int8 DEFAULT nextval('"User_id_Seq"'::regclass) NOT NULL,
    name text,
    "createTime" timestamp without time zone DEFAULT now(),
    --0:valid; other:disable
    status int2 DEFAULT 0,
    CONSTRAINT "PK_User" PRIMARY KEY (id)
) distribute by hash (id);

-- Name: User; Type: TABLE; Schema: public; Owner: -
DROP TABLE public."SpecialKey";
CREATE TABLE "SpecialKey" (
    "specialKey" text,
    CONSTRAINT "PK_SpecialKey" PRIMARY KEY ("specialKey")
) distribute by REPLICATION;

insert into "SpecialKey" values
(  '0000000000000000' ),
(  '353627055419637' ),
(  'Unknown' ),
(  '865813028484160' ),
(  '358021058396999' ),
(  '354833051995864' ),
(  '353919025680130' ),
(  '352315051641088' ),
(  '004999010640000' ),
(  '352751019523267' ),
(  '866499021834616' ),
(  '352558068271064' ),
(  '863388027165419' ),
(  '357507050008880' ),
(  '352273017386340' ),
(  '357138050105888' ),
(  '864691023017552' ),
(  '0' ),
(  '863853000853869' ),
(  '865316023019551' ),
(  '353627055435898' ),
(  '353627051096033' ),
(  '865454027871428' ),
(  '863777020094057' ),
(  '00000000000000' ),
(  '352315050191630' ),
(  '865454020905959' ),
(  '353627055419983' ),
(  '001068000000006' ),
(  '865813028725372' ),
(  '865372020520273' ),
(  '863388027219166' ),
(  '861622010000056' ),
(  '863880025407439' ),
(  '012345678912345' ),
(  '863673020337119' ),
(  '352956067858388' ),
(  '357138052978209' ),
(  '352315050812714' ),
(  '352061064911049' ),
(  '358688000000151' ),
(  '353627051225921' ),
(  '865198020249914' ),
(  '353627051217423' ),
(  '358021057835138' ),
(  '352315052232713' ),
(  '352558067456427' ),
(  '865454024309083' ),
(  '4a98fb7843065716' ),
(  '863388028478258' ),
(  '864375020364981' ),
(  '865703020518927' ),
(  '862949028781826' ),
(  '812345678912345' ),
(  '863925027236557' ),
(  '352315051613491' ),
(  '353627055435880' ),
(  '355167055711954' ),
(  '353627053034495' ),
(  '102790833675682' ),
(  '352558067456419' ),
(  '352315052230758' ),
(  '353627055437761' ),
(  '865316022605566' ),
(  '353627055435955' ),
(  '353627055435930' ),
(  '352315050357868' ),
(  '864595022088078' ),
(  '091646713467231' ),
(  '867731020001006' ),
(  '865056027472022' ),
(  '352315050651278' ),
(  '863388026653670' ),
(  '865982020088491' ),
(  '111111111111111' ),
(  '868969010014527' ),
(  '353627051156357' ),
(  '353627050361057' ),
(  '353627055433836' ),
(  '352315052820996' ),
(  '865372025155067' ),
(  '863092027178303' ),
(  '869725010108274' ),
(  '865411021639557' ),
(  '864254020043871' ),
(  '865424026383915' ),
(  '00000000' ),
(  '355899063508911' ),
(  '865372022646761' ),
(  '352315051014088' ),
(  '864375020099819' ),
(  '863990026964278' ),
(  '863077020069310' ),
(  '000000000000000' ),
(  '865072024328489' ),
(  '355167056120288' ),
(  '352558067456401' ),
(  '888888888888885' ),
(  '863990021234537' ),
(  '353719059790764' ),
(  '865454021627990' ),
(  '865372020070188' ),
(  '0123456789abcde' ),
(  '863990020631147' ),
(  '354273050379242' ),
(  '352315050235916' ),
(  '865198023938760' ),
(  '862594020568480' ),
(  '352005048247251' ),
(  '358673013795895' ),
(  '353627051217803' ),
(  '864981026638698' ),
(  '353627055433844' ),
(  '865407010000017' ),
(  '358380013510346' ),
(  '353627051174319' ),
(  '352558067456393' ),
(  '358688000000158' ),
(  '866328020021885' ),
(  '' )
;

