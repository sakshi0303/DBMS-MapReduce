-- Query 1: Task2  Get top five movie from 2000<=startyear<=2006 GENRES='Action,Drama'
set echo on 
spool 4331-5331_Proj3Spring23_team_31.txt
SELECT
    tb.PRIMARYTITLE,
    tr.AVERAGERATING,
    nb.PRIMARYNAME
FROM
    imdb00.TITLE_BASICS tb
    JOIN imdb00.TITLE_RATINGS tr ON tb.TCONST = tr.TCONST
    JOIN imdb00.TITLE_PRINCIPALS tp ON tb.TCONST = tp.TCONST
    AND tp.ORDERING = 1
    JOIN imdb00.NAME_BASICS nb ON tp.NCONST = nb.NCONST
WHERE
    tb.STARTYEAR >= 2000
    AND tb.STARTYEAR <= 2006
    AND tb.GENRES LIKE '%Action%'
    AND tb.GENRES LIKE '%Drama%'
    AND tr.NUMVOTES >= 100000
    AND tb.titletype = 'movie'
ORDER BY
    tr.AVERAGERATING DESC
FETCH FIRST
    5 ROWS ONLY;

EXPLAIN PLAN
SET
    statement_id = 'Query_1' FOR
SELECT
    tb.PRIMARYTITLE,
    tr.AVERAGERATING,
    nb.PRIMARYNAME
FROM
    imdb00.TITLE_BASICS tb
    JOIN imdb00.TITLE_RATINGS tr ON tb.TCONST = tr.TCONST
    JOIN imdb00.TITLE_PRINCIPALS tp ON tb.TCONST = tp.TCONST
    AND tp.ORDERING = 1
    JOIN imdb00.NAME_BASICS nb ON tp.NCONST = nb.NCONST
WHERE
    tb.STARTYEAR >= 2000
    AND tb.STARTYEAR <= 2006
    AND tb.GENRES LIKE '%Action%'
    AND tb.GENRES LIKE '%Drama%'
    AND tr.NUMVOTES >= 100000
    AND tb.titletype = 'movie'
ORDER BY
    tr.AVERAGERATING DESC
FETCH FIRST
    5 ROWS ONLY;

SELECT
    PLAN_TABLE_OUTPUT
FROM
    TABLE(DBMS_XPLAN.DISPLAY(NULL, 'Query_1', 'BASIC'));

SPOOL OFF
set echo off