-- Initial run
set max_parallel_workers_per_gather = 0;
set enable_hashjoin=on;
set enable_mergejoin=on;
set enable_nestloop=on;
set join_collapse_limit = 8;

VACUUM ANALYZE;

--BASE QUERY
EXPLAIN ANALYZE
SELECT 
    ed."eduLevel", 
    COUNT(ed.email) AS members
FROM 
    education ed
JOIN (
    SELECT 
        ad.email
    FROM 
        advertisement ad
    JOIN 
        "jobOffer" jo 
    ON 
        ad."advertisementID" = jo."advertisementID"
    WHERE 
        jo."fromAge" > 21 
        AND jo."fromAge" < 30
        AND AGE(ad."datePosted") <= '6 mons'
    GROUP BY 
        ad.email
    HAVING 
        COUNT(ad."advertisementID") >= 2
) adp
ON 
    adp.email = ed.email
JOIN (
    SELECT 
        "receiverEmail" as email
    FROM 
        msg
    WHERE 
        AGE(msg."dateSent") <= '6 mons'
    GROUP BY 
        "receiverEmail"
) rp
ON 
    rp.email = ed.email
WHERE 
    ed.country = 'Canada'
GROUP BY 
    ed."eduLevel"
ORDER BY members DESC; 
-- 984-996-989ms

--With Distinct (alternative of base)
EXPLAIN ANALYZE
SELECT 
    ed."eduLevel", 
    COUNT(DISTINCT ed.email) AS members
FROM 
    education ed
JOIN (
    SELECT 
        ad.email
    FROM 
        advertisement ad
    JOIN 
        "jobOffer" jo 
    ON 
        ad."advertisementID" = jo."advertisementID"
    WHERE 
        jo."fromAge" > 21 
        AND jo."fromAge" < 30
        AND AGE(ad."datePosted") <= '6 mons'
    GROUP BY 
        ad.email
    HAVING 
        COUNT(ad."advertisementID") >= 2
) adp
ON 
    adp.email = ed.email
JOIN 
	msg
ON 
    msg."receiverEmail" = ed.email
WHERE 
    ed.country = 'Canada'
	AND AGE("dateSent") <= '6 mons' 
GROUP BY 
    ed."eduLevel"
ORDER BY members DESC; 
-- 848-897-862 ms
-------------------------
-------------------------

VACUUM ANALYZE;

CREATE INDEX email_ad_ind_hash ON advertisement USING hash(email);
DROP INDEX email_ad_ind_hash;

CREATE INDEX email_ad_ind_btree ON advertisement USING btree(email);
DROP INDEX email_ad_ind_btree;


EXPLAIN ANALYZE
	SELECT 
	    ed."eduLevel", 
	    COUNT(ed.email) AS members
	FROM 
	    education ed
	JOIN (
	    SELECT 
	        ad.email
	    FROM 
	        advertisement ad
	    JOIN 
	        "jobOffer" jo 
	    ON 
	        ad."advertisementID" = jo."advertisementID"
	    WHERE 
	        jo."fromAge" > 21 
	        AND jo."fromAge" < 30
	        AND AGE(ad."datePosted") <= '6 mons'
	    GROUP BY 
	        ad.email
	    HAVING 
	        COUNT(ad."advertisementID") >= 2
	) adp
	ON 
	    adp.email = ed.email
	JOIN (
	    SELECT 
	        "receiverEmail" as email
	    FROM 
	        msg
	    WHERE 
	        AGE(msg."dateSent") <= '6 mons'
	    GROUP BY 
	        "receiverEmail"
	) rp
	ON 
	    rp.email = ed.email
	WHERE 
	    ed.country = 'Canada'
	GROUP BY 
	    ed."eduLevel"
	ORDER BY members DESC; 
-- 989-1028-1022 ms (with email_ad_ind_hash only)
-- 997-1035-1076 ms (with email_ad_ind_btree only)


VACUUM ANALYZE;

CREATE INDEX from_age_hash ON "jobOffer" USING hash("fromAge");
DROP INDEX from_age_hash;

CREATE INDEX from_age_btree ON "jobOffer" USING btree("fromAge");
DROP INDEX from_age_btree;


EXPLAIN ANALYZE
	SELECT 
	    ed."eduLevel", 
	    COUNT(ed.email) AS members
	FROM 
	    education ed
	JOIN (
	    SELECT 
	        ad.email
	    FROM 
	        advertisement ad
	    JOIN 
	        "jobOffer" jo 
	    ON 
	        ad."advertisementID" = jo."advertisementID"
	    WHERE 
	        jo."fromAge" > 21 
	        AND jo."fromAge" < 30
	        AND AGE(ad."datePosted") <= '6 mons'
	    GROUP BY 
	        ad.email
	    HAVING 
	        COUNT(ad."advertisementID") >= 2
	) adp
	ON 
	    adp.email = ed.email
	JOIN (
	    SELECT 
	        "receiverEmail" as email
	    FROM 
	        msg
	    WHERE 
	        AGE(msg."dateSent") <= '6 mons'
	    GROUP BY 
	        "receiverEmail"
	) rp
	ON 
	    rp.email = ed.email
	WHERE 
	    ed.country = 'Canada'
	GROUP BY 
	    ed."eduLevel"
	ORDER BY members DESC; 

-------------------------
-------------------------

VACUUM ANALYZE;

CREATE INDEX date_sent_hash ON "msg" USING hash("dateSent");
DROP INDEX date_sent_hash;

CREATE INDEX date_sent_btree ON "msg" USING btree("dateSent");
DROP INDEX date_sent_btree;


EXPLAIN ANALYZE
	SELECT 
	    ed."eduLevel", 
	    COUNT(ed.email) AS members
	FROM 
	    education ed
	JOIN (
	    SELECT 
	        ad.email
	    FROM 
	        advertisement ad
	    JOIN 
	        "jobOffer" jo 
	    ON 
	        ad."advertisementID" = jo."advertisementID"
	    WHERE 
	        jo."fromAge" > 21 
	        AND jo."fromAge" < 30
	        AND AGE(ad."datePosted") <= '6 mons'
	    GROUP BY 
	        ad.email
	    HAVING 
	        COUNT(ad."advertisementID") >= 2
	) adp
	ON 
	    adp.email = ed.email
	JOIN (
	    SELECT 
	        "receiverEmail" as email
	    FROM 
	        msg
	    WHERE 
	        AGE(msg."dateSent") <= '6 mons'
	    GROUP BY 
	        "receiverEmail"
	) rp
	ON 
	    rp.email = ed.email
	WHERE 
	    ed.country = 'Canada'
	GROUP BY 
	    ed."eduLevel"
	ORDER BY members DESC; 




VACUUM ANALYZE;

CREATE INDEX email_receiver_hash ON "msg" USING hash("receiverEmail");
DROP INDEX email_receiver_hash;

CREATE INDEX email_receiver_btree ON "msg" USING btree("receiverEmail");
DROP INDEX email_receiver_btree;


EXPLAIN ANALYZE
	SELECT 
	    ed."eduLevel", 
	    COUNT(ed.email) AS members
	FROM 
	    education ed
	JOIN (
	    SELECT 
	        ad.email
	    FROM 
	        advertisement ad
	    JOIN 
	        "jobOffer" jo 
	    ON 
	        ad."advertisementID" = jo."advertisementID"
	    WHERE 
	        jo."fromAge" > 21 
	        AND jo."fromAge" < 30
	        AND AGE(ad."datePosted") <= '6 mons'
	    GROUP BY 
	        ad.email
	    HAVING 
	        COUNT(ad."advertisementID") >= 2
	) adp
	ON 
	    adp.email = ed.email
	JOIN (
	    SELECT 
	        "receiverEmail" as email
	    FROM 
	        msg
	    WHERE 
	        AGE(msg."dateSent") <= '6 mons'
	    GROUP BY 
	        "receiverEmail"
	) rp
	ON 
	    rp.email = ed.email
	WHERE 
	    ed.country = 'Canada'
	GROUP BY 
	    ed."eduLevel"
	ORDER BY members DESC; 
