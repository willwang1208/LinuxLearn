DROP ROLE mfpay;
CREATE ROLE mfpay LOGIN PASSWORD 'microfun_2016';

DROP DATABASE mfpay;
CREATE DATABASE mfpay OWNER mfpay ENCODING 'UTF8';

--
-- TOC entry 193 (class 1255 OID 32937)
-- Name: ACPaidProof(int4, int8, int4, int4, int4, int4, int2, text, text, int2, timestamp without time zone); Type: FUNCTION; Schema: public; Owner: -
--
DROP FUNCTION public."ACPaidProof"(integer, bigint, integer, integer, text, smallint, text, text, smallint, timestamp without time zone);
CREATE or replace FUNCTION "ACPaidProof"(
    "appId" int4, 
    "userId" int8, 
    "channelId" int4, 
    "payChannelId" int4,
    "productId" text,
    "payResult" int2, 
    "receiptData" text, 
    "receiptSignature" text, 
    "verifyResult" int2,
    ctime timestamp without time zone) 
RETURNS TABLE("resultCode" int4)
LANGUAGE plpgsql
AS $$
  declare
    "oldCtime" timestamp without time zone;
    "productPrice" float4;
  begin

    select
        p.ctime
    into
        "oldCtime"
    from 
        "Payment" p
    where 
        p."appId" = "ACPaidProof"."appId"
        and
        p."userId" = "ACPaidProof"."userId"
        and
        p.ctime = "ACPaidProof".ctime
    ;
    if not found then
        if "ACPaidProof"."productId" != '' and "ACPaidProof"."payResult" = 0 then
            select
                d.price
            into
                "productPrice"
            from 
                "Product" d
            where 
                d."productId" = "ACPaidProof"."productId"
            ;
        else
            "productPrice" = 0
            ;
        end if
        ;

        insert into "Payment" (
        "appId",
        "userId",
        "channelId",
        "payChannelId",
        "productId",
        money,
        "payResult",
        "receiptData",
        "receiptSignature",
        "verifyResult",
        ctime
        )
        values(
        "ACPaidProof"."appId",
        "ACPaidProof"."userId",	
        "ACPaidProof"."channelId",
        "ACPaidProof"."payChannelId",
        "ACPaidProof"."productId",
        "productPrice",		
        "ACPaidProof"."payResult",
        "ACPaidProof"."receiptData",
        "ACPaidProof"."receiptSignature",
        "ACPaidProof"."verifyResult",
        "ACPaidProof".ctime
        )
        ;
        return query select 0 "resultCode"; 
    else
        return query select 1 "resultCode"; 
    end if;

  end;
$$;


--
-- 
-- Name: Order_id_Seq; Type: SEQUENCE; Schema: public; Owner: -
--
DROP SEQUENCE public."Order_id_Seq";
CREATE SEQUENCE "Order_id_Seq"
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- 
-- Name: Payment; Type: TABLE; Schema: public; Owner: -
--
DROP TABLE public."Payment";
CREATE TABLE "Payment" (
    id int8 DEFAULT nextval('"Order_id_Seq"'::regclass) NOT NULL,
    "appId" int4 NOT NULL,
    "userId" int8 NOT NULL,
    "channelId" int4 NOT NULL,
    "payChannelId" int4 NOT NULL,
    "productId" text NOT NULL,
    money float4 NOT NULL,
    -- 0:success;other:fail
    "payResult" int2 NOT NULL,
    "receiptData" text,
    "receiptSignature" text,
    -- 0:success;other:fail
    "verifyResult" int2 NOT NULL,
    ctime timestamp without time zone NOT NULL,
    CONSTRAINT "PK_Payment" PRIMARY KEY (id)
) distribute by hash(id);

DROP TABLE public."Product";
CREATE TABLE "Product" (
    "productId" text NOT NULL,
    "productName" text,
    price float4 NOT NULL,
    type text,
    --0:valid; other:disable
    status int2 DEFAULT 0,
    ctime timestamp without time zone DEFAULT now(),
    CONSTRAINT "PK_Product" PRIMARY KEY ("productId")
) distribute by REPLICATION;


insert into "Product"
("productId","productName",price,status)
values
('com.microfun.monsters.gold1','10',0.99,0),
('com.microfun.monsters.gold2','55',4.99,0),
('com.microfun.monsters.gold3','120',9.99,0),
('com.microfun.monsters.gold4','250',19.99,0),
('com.microfun.monsters.gold5','650',49.99,0);

insert into "Product"
("productId","productName",price,status)
values
('com.ymg.monsterlab.gold1','10',0.99,0),
('com.ymg.monsterlab.gold2','55',4.99,0),
('com.ymg.monsterlab.gold3','120',9.99,0),
('com.ymg.monsterlab.gold4','250',19.99,0),
('com.ymg.monsterlab.gold5','650',49.99,0);

insert into "Product"
("productId","productName",price,status)
values
('com.ymg.monsterblast.gold1','10',0.99,0),
('com.ymg.monsterblast.gold2','55',4.99,0),
('com.ymg.monsterblast.gold3','120',9.99,0),
('com.ymg.monsterblast.gold4','250',19.99,0),
('com.ymg.monsterblast.gold5','650',49.99,0);
