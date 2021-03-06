DROP TABLE IF EXISTS REPOS_QUEUE;

CREATE TABLE REPOS_QUEUE (
	REPO_OWNER 	VARCHAR(50) NOT NULL,
	REPOSITORY 	VARCHAR(100) NOT NULL,
    CHECKOUT_ID VARCHAR(13),
    RESULT 		VARCHAR(255),
    COMPLETED 	BOOLEAN DEFAULT FALSE
)
ROW_FORMAT=COMPRESSED
KEY_BLOCK_SIZE=8;

ALTER TABLE REPOS_QUEUE ADD PRIMARY KEY (REPO_OWNER, REPOSITORY);

CREATE OR REPLACE VIEW PMON AS
(SELECT 'TIME'AS STATUS, NOW() AS DETAILS, 0 AS 'COUNT(*)') UNION ALL
(SELECT
  CASE
    WHEN(RESULT IS NOT NULL) THEN 'ERROR'
	WHEN(COMPLETED=1) THEN 'FINISHED'
	WHEN(CHECKOUT_ID IS NOT NULL) THEN 'IN_PROGRESS'
	ELSE 'PENDING'
  END AS STATUS,
  ''  AS DETAILS,
  COUNT(*)
FROM
    REPOS_QUEUE
GROUP BY 1, 2) UNION ALL
(SELECT
	'IN_PROGRESS',
    CHECKOUT_ID,
	COUNT(*)
FROM
	REPOS_QUEUE
WHERE COMPLETED = 0 AND CHECKOUT_ID IS NOT NULL AND RESULT IS NULL
GROUP BY 1 , 2
ORDER BY 2 DESC) UNION ALL
(SELECT
	'ERROR',
    CASE
      WHEN (RESULT LIKE '%No such file or directory%')     THEN 'No such file or directory'
      WHEN (RESULT LIKE '%503 Service Unavailable%')       THEN '503 Service Unavailable'
      WHEN (RESULT LIKE '%cannot open git-upload-pack%')   THEN 'cannot open git-upload-pack'
      WHEN (RESULT LIKE '%git-upload-pack not permitted%') THEN 'git-upload-pack not permitted'
      WHEN (RESULT LIKE '%authentication not supported%')  THEN 'authentication not supported'
      WHEN (RESULT LIKE '%Duplicate stages not allowed:%') THEN 'Duplicate stages not allowed'
      WHEN (RESULT LIKE '%File name too long%')            THEN 'File name too long'
      WHEN (RESULT LIKE '%Nul character not allowed:%')    THEN 'Nul character not allowed'
      WHEN (RESULT LIKE '%No space left on device%')       THEN 'No space left on device'
      ELSE SUBSTRING(RESULT, 1, 50)
    END,
	COUNT(*)
FROM
	REPOS_QUEUE
WHERE RESULT IS NOT NULL
GROUP BY 1 , 2
ORDER BY 3 DESC);

SELECT * FROM PMON;

UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%authentication not supported%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%503 Service Unavailable%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%SSL peer shut down incorrectly%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%No such file or directory%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%Shutdown in progress%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%Socket%closed%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%No space left on device%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%Read timed out after 120,000 ms%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%SSL peer shut down incorrectly%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%git-upload-pack not permitted%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%Connection timed out (Connection timed out)%';
UPDATE REPOS_QUEUE SET RESULT=null, COMPLETED=0, CHECKOUT_ID=null WHERE RESULT LIKE '%cannot open git-upload-pack%';

UPDATE REPOS_QUEUE SET CHECKOUT_ID=null WHERE COMPLETED=0;
UPDATE REPOS_QUEUE SET CHECKOUT_ID=null WHERE COMPLETED=0 AND CHECKOUT_ID NOT IN (1528559497683);