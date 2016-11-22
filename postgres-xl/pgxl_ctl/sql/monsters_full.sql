DROP ROLE monsters;
CREATE ROLE monsters LOGIN PASSWORD 'microfun_2016';

DROP DATABASE monsters;
CREATE DATABASE monsters OWNER monsters ENCODING 'UTF8';

DROP TYPE public."PlayRecord";
SELECT oid from pg_type where typname='PlayRecord';
CREATE TYPE "PlayRecord" AS (
    level int4,
    score int4,
    stars int4
);

DROP TYPE public."BranchPlayRecord";
SELECT oid from pg_type where typname='BranchPlayRecord';
CREATE TYPE "BranchPlayRecord" AS (
    branch int4,
    level int4,
    score int4,
    stars int4
);

DROP TYPE public."Props";
SELECT oid from pg_type where typname='Props';
CREATE TYPE "Props" AS (
    "goldBalance" int4,
    bomb1 int4,
    bomb9 int4,
    "bombCross" int4,
    "bombColor" int4
);


DROP FUNCTION public."BLGetSyncFlag"(bigint);
CREATE or replace FUNCTION "BLGetSyncFlag"("userId" int8) 
    RETURNS TABLE("syncTime" int8, "installationId" text, stime timestamp without time zone)
    LANGUAGE plpgsql
    AS $$
declare
    "oldSyncTime" int8;
    "oldInstallationId" text;
    "serverTime" timestamp without time zone = now();
begin
    select
        s."syncTime", s."installationId"
    into
        "oldSyncTime",
        "oldInstallationId"
    from 
        "UserSync" s
    where 
        s."userId" = "BLGetSyncFlag"."userId"
    ;
    if not found then
        return query select 0::int8 "syncTime", "oldInstallationId" "installationId", "serverTime" stime; 
    else
        return query select "oldSyncTime" "syncTime",  "oldInstallationId" "installationId", "serverTime" stime;  
    end if;
end;
$$;

DROP FUNCTION public."BLGetUser"(bigint);
CREATE or replace FUNCTION "BLGetUser"("userId" int8) 
    RETURNS TABLE("syncTime" int8, levels "PlayRecord"[], props "Props")
    LANGUAGE plpgsql
    AS $$
declare
    "syncTimeL" int8 = 0;
    "propsL" "Props";
begin
    select 
        s."syncTime"
    into 
        "syncTimeL"
    from "UserSync" s where s."userId" = "BLGetUser"."userId"
    ;

    select 
        "goldBalance", bomb1, bomb9, "bombCross", "bombColor"
    into 
        "propsL"."goldBalance",  "propsL"."bomb1", "propsL"."bomb9", "propsL"."bombCross", "propsL"."bombColor"
    from "UserAccount" a where a."userId" = "BLGetUser"."userId"
    ;

    return query select
        "syncTimeL" "syncTime",
        array
        (
            select
            row
            (
                level,
                score,
                stars
            )::"PlayRecord"
            from "PassedLevel" l
            where l."userId" = "BLGetUser"."userId"
        )::"PlayRecord"[] "levels",
        "propsL" props
    ;
end;
$$;

DROP FUNCTION public."BLGetUserAll"(bigint);
CREATE or replace FUNCTION "BLGetUserAll"("userId" int8) 
    RETURNS TABLE("syncTime" int8, "levels" "BranchPlayRecord"[], props "Props")
    LANGUAGE plpgsql
    AS $$
declare
    "syncTimeL" int8 = 0;
    "propsL" "Props";
begin
    select 
        s."syncTime"
    into 
        "syncTimeL"
    from "UserSync" s where s."userId" = "BLGetUserAll"."userId"
    ;

    select 
        "goldBalance", bomb1, bomb9, "bombCross", "bombColor"
    into 
        "propsL"."goldBalance",  "propsL"."bomb1", "propsL"."bomb9", "propsL"."bombCross", "propsL"."bombColor"
    from "UserAccount" a where a."userId" = "BLGetUserAll"."userId"
    ;

    return query select
        "syncTimeL" "syncTime",
        array
        (
            select
            row
            (
                branch,
                level,
                score,
                stars
            )::"BranchPlayRecord"
            from "PassedLevel" l
            where l."userId" = "BLGetUserAll"."userId"
        )::"BranchPlayRecord"[] "levels",
        "propsL" props
    ;
end;
$$;

DROP FUNCTION public."BLUpdUser"(bigint, bigint, text, "PlayRecord"[], "Props");
CREATE or replace FUNCTION "BLUpdUser"( "userId" int8, "syncTime" int8, "installationId" text, "levels" "PlayRecord"[], props "Props")
    --resultCode:0-success;1-fail
    RETURNS TABLE("resultCode" int4, "newSyncTime" int8)
	SET client_min_messages = error
    LANGUAGE plpgsql
    AS $$
declare
    "maxStars" int4;
    "maxScore" int4;
    "hasChanged" int2 = 0;
    "oldSyncTime" int8;
    "oldInstallationId" text;
    "oldGoldBalance" int4;
    "newSyncTimeL" int8;
    "isExist" int2 = 1;
    i int4;
begin
    -- If no such user, create and notify background worker to sync info from SSO
    
    -- As we only keep the highest record, the message Id actually makes no use to us

    -- Process message by inserting or updating level record

    -- check if can update
    select
        s."syncTime", s."installationId"
    into
        "oldSyncTime",
        "oldInstallationId"
    from 
        "UserSync" s
    where 
        s."userId" = "BLUpdUser"."userId"
    ;
    -- the first upload
    if not found then
        "isExist" = 0;
    elsif "BLUpdUser"."syncTime" != "oldSyncTime" then
        return query select 1 "resultCode",  "BLUpdUser"."syncTime" "newSyncTime";  
        return;
    end if;

    create temp table if not exists tt
    (
        "userId" int8, level integer, score integer, stars integer
    );

    truncate table tt;

    insert into tt("userId", level, score, stars)
    select "BLUpdUser"."userId", r.*
    from unnest(levels) r;
    
    INSERT INTO "PassedLevel"
    SELECT *, now()
    from 
    tt x
    where not exists(
    select 1 from "PassedLevel" p
    where p."userId" = x."userId" and p.level = x.level
    );

    UPDATE "PassedLevel" p
    SET 
    score = GREATEST(x.score, p.score),
    stars = GREATEST(x.stars, p.stars)
    from
    tt x
    where
    p."userId" = x."userId"
    and
    p.level = x.level
    ;

    -- update "UserAccount"

    -- If no such user, create and notify background worker to sync info from SSO

    select 
        a."goldBalance"
    into 
        "oldGoldBalance"
    from 
        "UserAccount" a
    where 
        a."userId" = "BLUpdUser"."userId"
    ;

    -- Process message by inserting or updating balance record
    if not found then
        insert into "UserAccount" (
            "userId",     
            "goldBalance",  
            bomb1,
            bomb9,
            "bombCross",
            "bombColor",
            "ctime"       
            )
        values   (
            "BLUpdUser"."userId", 
            "BLUpdUser".props."goldBalance", 
            "BLUpdUser".props.bomb1, 
            "BLUpdUser".props.bomb9, 
            "BLUpdUser".props."bombCross",
            "BLUpdUser".props."bombColor", 
            now()
            )		
        ;
    else
        update "UserAccount" a 
        set
            "goldBalance" = "BLUpdUser".props."goldBalance",
            "bomb1" = "BLUpdUser".props.bomb1,
            "bomb9" = "BLUpdUser".props.bomb9,
            "bombCross" = "BLUpdUser".props."bombCross",
            "bombColor" = "BLUpdUser".props."bombColor",
            "ctime" = now()
        where 
            a."userId" = "BLUpdUser"."userId"
        ;
    end if;

    -- update "UserSync"
    "newSyncTimeL" = extract(epoch from now()- '2015-01-01 00:00:00')::int8;
    if "isExist" = 1 then
          update "UserSync" s 
          set
            "installationId" = "BLUpdUser"."installationId",
            "syncTime" = "newSyncTimeL",
            "ctime" = now()
          where 
            s."userId" = "BLUpdUser"."userId"
          ;
    else
        insert into "UserSync" (           
            "userId",             
            "installationId",               
            "syncTime",               
            "ctime"                     
            )
        values(
            "BLUpdUser"."userId",   
            "BLUpdUser"."installationId",    
            "newSyncTimeL",    
            now()
            )
        ;                     
    end if
    ;
    return query select 0 "resultCode",  "newSyncTimeL" "newSyncTime";  
end;
$$;

DROP FUNCTION public."BLUpdUserBranch"(bigint, bigint, text, "BranchPlayRecord"[], "Props");
CREATE or replace FUNCTION "BLUpdUserBranch"( "userId" int8, "syncTime" int8, "installationId" text, "levels" "BranchPlayRecord"[], props "Props")
    --resultCode:0-success;1-fail
    RETURNS TABLE("resultCode" int4, "newSyncTime" int8)
	SET client_min_messages = error
    LANGUAGE plpgsql
    AS $$
declare
    "maxStars" int4;
    "maxScore" int4;
    "hasChanged" int2 = 0;
    "oldSyncTime" int8;
    "oldInstallationId" text;
    "oldGoldBalance" int4;
    "newSyncTimeL" int8;
    "isExist" int2 = 1;
    i int4;
begin
    -- If no such user, create and notify background worker to sync info from SSO
    
    -- As we only keep the highest record, the message Id actually makes no use to us

    -- Process message by inserting or updating level record

    -- check if can update
    select
        s."syncTime", s."installationId"
    into
        "oldSyncTime",
        "oldInstallationId"
    from 
        "UserSync" s
    where 
        s."userId" = "BLUpdUserBranch"."userId"
    ;
    -- the first upload
    if not found then
        "isExist" = 0;
    elsif "BLUpdUserBranch"."syncTime" != "oldSyncTime" then
        return query select 1 "resultCode",  "BLUpdUserBranch"."syncTime" "newSyncTime";  
        return;
    end if;

    create temp table if not exists ttb
    (
        "userId" int8, branch integer, level integer, score integer, stars integer
    );

    truncate table ttb;

    insert into ttb("userId", branch, level, score, stars)
    select "BLUpdUserBranch"."userId", r.*
    from unnest(levels) r;
    
    INSERT INTO "PassedLevel"("userId", branch, level, score, stars, ctime)
    SELECT *, now()
    from 
    ttb x
    where not exists(
    select 1 from "PassedLevel" p
    where p."userId" = x."userId" and p.branch = x.branch and p.level = x.level
    );

    UPDATE "PassedLevel" p
    SET 
    score = GREATEST(x.score, p.score),
    stars = GREATEST(x.stars, p.stars)
    from
    ttb x
    where
    p."userId" = x."userId"
    and
    p.branch = x.branch
    and
    p.level = x.level
    ;

    -- update "UserAccount"

    -- If no such user, create and notify background worker to sync info from SSO

    select 
        a."goldBalance"
    into 
        "oldGoldBalance"
    from 
        "UserAccount" a
    where 
        a."userId" = "BLUpdUserBranch"."userId"
    ;

    -- Process message by inserting or updating balance record
    if not found then
        insert into "UserAccount" (
            "userId",     
            "goldBalance",  
            bomb1,
            bomb9,
            "bombCross",
            "bombColor",
            "ctime"       
            )
        values   (
            "BLUpdUserBranch"."userId", 
            "BLUpdUserBranch".props."goldBalance", 
            "BLUpdUserBranch".props.bomb1, 
            "BLUpdUserBranch".props.bomb9, 
            "BLUpdUserBranch".props."bombCross",
            "BLUpdUserBranch".props."bombColor", 
            now()
            )		
        ;
    else
        update "UserAccount" a 
        set
            "goldBalance" = "BLUpdUserBranch".props."goldBalance",
            "bomb1" = "BLUpdUserBranch".props.bomb1,
            "bomb9" = "BLUpdUserBranch".props.bomb9,
            "bombCross" = "BLUpdUserBranch".props."bombCross",
            "bombColor" = "BLUpdUserBranch".props."bombColor",
            "ctime" = now()
        where 
            a."userId" = "BLUpdUserBranch"."userId"
        ;
    end if;

    -- update "UserSync"
    "newSyncTimeL" = extract(epoch from now()- '2015-01-01 00:00:00')::int8;
    if "isExist" = 1 then
          update "UserSync" s 
          set
            "installationId" = "BLUpdUserBranch"."installationId",
            "syncTime" = "newSyncTimeL",
            "ctime" = now()
          where 
            s."userId" = "BLUpdUserBranch"."userId"
          ;
    else
        insert into "UserSync" (           
            "userId",             
            "installationId",               
            "syncTime",               
            "ctime"                     
            )
        values(
            "BLUpdUserBranch"."userId",   
            "BLUpdUserBranch"."installationId",    
            "newSyncTimeL",    
            now()
            )
        ;                     
    end if
    ;
    return query select 0 "resultCode",  "newSyncTimeL" "newSyncTime";  
end;
$$;

DROP TABLE public."PassedLevel";
CREATE TABLE "PassedLevel" (
    "userId" int8 NOT NULL,
    branch int4,
    level int4 NOT NULL,
    score int4 NOT NULL,
    stars int4 NOT NULL,
    "ctime" timestamp without time zone DEFAULT now(),
    CONSTRAINT "PK_PassedLevel" PRIMARY KEY ("userId", level)
) distribute by hash("userId");

DROP TABLE public."UserAccount";
CREATE TABLE "UserAccount" (
    "userId" int8 NOT NULL,
    "goldBalance" int4 NOT NULL, 
    "bomb1" int4 NOT NULL,
    "bomb9" int4 NOT NULL,
    "bombCross" int4 NOT NULL,
    "bombColor" int4 NOT NULL,
    "ctime" timestamp without time zone DEFAULT now(),
    CONSTRAINT "PK_UserAccount" PRIMARY KEY ("userId")
) distribute by hash("userId");

DROP TABLE public."UserSync";
CREATE TABLE "UserSync" (
    "userId" int8 NOT NULL,
    "syncTime" int8 NOT NULL,
    "installationId" text NOT NULL,	
    "ctime" timestamp without time zone DEFAULT now(),
    CONSTRAINT "PK_UserSync" PRIMARY KEY ("userId")
) distribute by hash("userId");