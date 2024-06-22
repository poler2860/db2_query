-- Initial run
set max_parallel_workers_per_gather = 0;
set enable_hashjoin=on;
set enable_mergejoin=on;

VACUUM ANALYZE;

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
-- 1s

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
-- 860 ms
-------------------------
-- Using a btree index on 
-------------------------
VACUUM ANALYZE;

CREATE INDEX email_ad_ind ON advertisement USING hash(email);
DROP INDEX email_ad_ind;

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
-- 872 ms (with email_ad_ind only)
