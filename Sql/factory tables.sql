create database Factory ;

use factory ;

CREATE TABLE IF NOT EXISTS Worker(
    sub_ID INT,
    Full_name VARCHAR(255),
    sub_age INT,
    sub_sex VARCHAR(10),
    sub_shift VARCHAR(20),
    sub_team VARCHAR(50),
    sub_role VARCHAR(100),
    sup_ID INT,      -- Isme '0' aur 'None' handle ho jayenge
    Full_Name_Sup VARCHAR(255),
    sup_role VARCHAR(100),
    event_dat DATE,
    event_wee VARCHAR(20),
    behav_con VARCHAR(255),
    actual_eff DECIMAL(10,6),
    recorded_ DECIMAL(10,6),
    record_co VARCHAR(100),
    behav_cause_h TEXT
);


LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/factory data1.csv'
INTO TABLE Worker
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(
sub_ID,
Full_name,
sub_age,
sub_sex,
sub_shift,
sub_team,
sub_role,
@sup_ID,
Full_Name_Sup,
sup_role,
@event_dat,
event_wee,
behav_con,
@actual_eff,
@recorded_,
record_co,
behav_cause_h
)
SET
sup_ID = NULLIF(NULLIF(@sup_ID, ''), '0'),   -- 🔥 FIX
event_dat = STR_TO_DATE(@event_dat, '%m/%d/%Y'),
actual_eff = NULLIF(@actual_eff, ''),
recorded_ = NULLIF(@recorded_, '');



CREATE TABLE dim_emp (PRIMARY KEY (sub_ID)) 
AS 
SELECT DISTINCT 
    sub_ID, 
    Full_name AS Sub_Fname, 
    sub_age, 
    sub_sex, 
    sub_role, 
    sub_shift, 
    sub_team
FROM worker;


SELECT * FROM Dim_Emp ;


CREATE TABLE dim_sup (PRIMARY KEY (sup_key)) 
AS 
SELECT 
    CONCAT('sup', ROW_NUMBER() OVER (ORDER BY sup_ID)) AS sup_key,
    sup_ID, 
    Full_Name_Sup AS sup_Fname, 
    sup_role
FROM (
    SELECT DISTINCT 
        sup_ID, 
        Full_Name_Sup, 
        sup_role
    FROM worker
) AS unique_sups;


SELECT * FROM Dim_sup ;


CREATE TABLE dim_date (PRIMARY KEY (Date_ID)) 
AS 
SELECT DISTINCT 
    CAST(DATE_FORMAT(event_dat, '%Y%m%d') AS UNSIGNED) AS Date_ID,
    event_dat AS Full_Date,
    DAYNAME(event_dat) AS Day_Name,
    MONTHNAME(event_dat) AS Month_Name,
    YEAR(event_dat) AS Year_val
FROM worker;

SELECT * FROM dim_date ;

CREATE TABLE fact_worker (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    sub_id INT ,
    sup_key VARCHAR(25),
    date_id BIGINT UNSIGNED,
    behav_con VARCHAR(255),
    actual_eff DECIMAL(10,8),
    recorded_ DECIMAL(10,8),
    record_con_matrix VARCHAR(100),
    behav_cause_h TEXT,

    CONSTRAINT fk_emp FOREIGN KEY (sub_id) REFERENCES dim_emp(sub_id),
    CONSTRAINT fk_sup FOREIGN KEY (sup_key) REFERENCES dim_sup(sup_key),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

INSERT INTO fact_worker (
    sub_id,
    sup_key,
    date_id,
    behav_con,
    actual_eff,
    recorded_,
    record_con_matrix,
    behav_cause_h
)
SELECT 
    w.sub_ID,
    ds.sup_key,
    dd.date_id,
    w.behav_con,

    CASE 
        WHEN w.actual_eff REGEXP '^[0-9.]+$' THEN CAST(w.actual_eff AS DECIMAL(10,8))
        ELSE NULL 
    END,

    CASE 
        WHEN w.recorded_ REGEXP '^[0-9.]+$' THEN CAST(w.recorded_ AS DECIMAL(10,8))
        ELSE NULL 
    END,

    w.record_co,
    w.behav_cause_h

FROM Worker w
LEFT JOIN dim_sup ds ON w.sup_ID = ds.sup_ID
LEFT JOIN dim_date dd ON w.event_dat = dd.full_date;


UPDATE Worker 
SET actual_eff = NULL
WHERE TRIM(CAST(actual_eff AS CHAR)) = '';

UPDATE Worker 
SET recorded_ = NULL
WHERE TRIM(CAST(recorded_ AS CHAR)) = '';


SELECT * FROM fact_worker ;

DELETE FROM fact_worker 
WHERE fact_Id BETWEEN 411949 AND 411986 ;

-- ------------------------------------------
-- KPI 

-- 1. avg efficience actual
SELECT avg(actual_eff) FROM fact_worker ;

-- 2. Total Worker
SELECT count(*) FROM dim_emp ;

-- 3. Total supevisor 
SELECT count(*) FROM dim_sup ;

-- 4. avg efficience recorded
SELECT avg(recorded_) FROM fact_worker ;

-- 5. avg eff - recorded 
SELECT (avg(actual_eff)- avg(recorded_)) FROM fact_worker  ;

-- ----------------------------------------
-- 1. chart sub roal wise 
SELECT sub_role , count(sub_id) from dim_emp 
GROUP BY sub_role ;

-- 2. Day Wise Actual Efficience
SELECT 
    d.day_name, 
    ROUND(AVG(f.actual_eff), 8) AS avg_by_day 
FROM fact_worker f
JOIN dim_date d ON f.date_id = d.date_id 
GROUP BY d.day_name 
ORDER BY 
    FIELD(d.day_name, 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');

-- 3. Ave efficience by month wise
SELECT d.month_name,
	round(avg(actual_eff),8) AS Avg_By_Month
    FROM fact_worker f
    join dim_date d on f.Date_ID = d.Date_ID
    GROUP BY d.month_name
    ORDER BY
    field(d.month_name,'January', 'February', 
    'March', 'April', 'May', 'June', 'July', 'August', 
    'September', 'October', 'November', 'December') ;

-- 4. male and female

SELECT sub_sex,count(sub_id) FROM dim_emp
GROUP BY sub_sex ;

-- 5. male and femal actual efficience

SELECT sub_sex,avg(actual_eff) FROM fact_worker f 
join dim_emp e on e.sub_id=f.sub_id
GROUP BY sub_sex ;

-- 6. shift wise eff
SELECT e.sub_shift , avg(actual_eff) from fact_worker f 
join dim_emp e on e.sub_id=f.sub_id
GROUP BY e.sub_shift ;

-- 7. roal wise avg eff
SELECT e.sub_role , avg(actual_eff) from fact_worker f 
join dim_emp e on e.sub_id=f.sub_id
GROUP BY e.sub_role ;

-- 8. avg eff by team wise
SELECT e.sub_team , avg(actual_eff) from fact_worker f 
join dim_emp e on e.sub_id=f.sub_id
GROUP BY e.sub_team ;

-- 9. age wise avg eff
SELECT e.sub_age , avg(actual_eff) from fact_worker f 
join dim_emp e on e.sub_id=f.sub_id
GROUP BY e.sub_age
ORDER BY e.sub_age ASC ;

-- 10. record con matrx wise work checking or not or fale rating 
SELECT 
    record_con_matrix, 
    COUNT(record_con_matrix) as total_count
FROM fact_worker
WHERE behav_con IS not NULL 
  AND behav_con  not IN ('None', 'Absence', 'Presence',"Resignation") 
GROUP BY record_con_matrix;

-- 11. emp resignation by reason 
SELECT 
    behav_cause_h AS Resignation_Reason, 
    COUNT(*) AS Resignation_Count
FROM fact_worker
WHERE behav_cause_h IS NOT NULL 
  AND TRIM(behav_cause_h) NOT LIKE 'None%' -- 'None' se shuru hone wala sab khatam
  AND TRIM(behav_cause_h) != '' 
GROUP BY behav_cause_h
ORDER BY Resignation_Count DESC;


-- 12. top 10 supervise performance
SELECT s.sup_Fname, avg(actual_eff) from dim_sup s
join fact_worker f on  s.sup_key=f.sup_key 
WHERE f.behav_con != 'resignation'
GROUP BY s.sup_Fname
ORDER BY avg(actual_eff) DESC
LIMIT 10 ;


-- 13. bottom 10 supervise performance
SELECT s.sup_Fname, avg(actual_eff) from dim_sup s
join fact_worker f on  s.sup_key=f.sup_key 
WHERE f.behav_con != 'resignation'
GROUP BY s.sup_Fname
ORDER BY avg(actual_eff) ASC
LIMIT 10 ;

-- 14. top 10 emp by eff
SELECT s.sub_Fname, avg(actual_eff) from dim_emp s
join fact_worker f on  s.sub_id=f.sub_id 
WHERE f.behav_con != 'resignation'
GROUP BY s.sub_Fname
ORDER BY avg(actual_eff) DESC
LIMIT 10 ;


-- 14. bottom 10 emp by eff
SELECT s.sub_Fname, avg(actual_eff) from dim_emp s
join fact_worker f on  s.sub_id=f.sub_id 
WHERE f.behav_con != 'resignation'
GROUP BY s.sub_Fname
ORDER BY avg(actual_eff) ASC
LIMIT 10 ;

-- 15. age wise grouping

SELECT 
    CASE
        WHEN e.sub_age < 25 THEN 'Under 25'
        WHEN e.sub_age BETWEEN 25 AND 40 THEN '25-40'
        WHEN e.sub_age BETWEEN 41 AND 55 THEN '41-55'
        WHEN e.sub_age >= 56 THEN '56+'
        ELSE 'Unknown'
    END AS Age_Group,

    ROUND(AVG(f.actual_eff), 4) AS Avg_Actual_Efficiency

FROM fact_worker f
JOIN dim_emp e 
     ON f.sub_id = e.sub_id

WHERE f.actual_eff IS NOT NULL

GROUP BY Age_Group
ORDER BY 
    FIELD(Age_Group,'Under 25','25-40','41-55','56+');


