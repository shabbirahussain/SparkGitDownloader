USE reactorlabs_2018_02_01;

#DROP TABLE REPOS_QUEUE;
CREATE TABLE REPOS_QUEUE (
	REPO_OWNER 	VARCHAR(255) NOT NULL,
	REPOSITORY 	VARCHAR(255) NOT NULL,
    BRANCH 			VARCHAR(255) DEFAULT 'master',
    CHECKOUT_ID 	VARCHAR(255),
    RESULT 				VARCHAR(255),
    COMPLETED 		BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (REPO_OWNER, REPOSITORY, BRANCH),
    INDEX IDX_REPOS_QUEUE_COMPLETED (COMPLETED)
)
ROW_FORMAT=COMPRESSED
KEY_BLOCK_SIZE=8;


#DROP TABLE FILE_HISTORY_QUEUE;
-- CREATE TABLE FILE_HISTORY_QUEUE (
-- 	REPO_OWNER 	VARCHAR(255) NOT NULL,
-- 	REPOSITORY 	VARCHAR(255) NOT NULL,
--     BRANCH 			VARCHAR(255) DEFAULT 'master',
-- 	GIT_PATH 			VARCHAR(255) NOT NULL,
--     CHECKOUT_ID 	VARCHAR(255),
--     RESULT 				VARCHAR(255),
--     COMPLETED 		BOOLEAN DEFAULT FALSE
--     # PRIMARY KEY (REPO_OWNER, REPOSITORY, BRANCH, GIT_PATH)
-- )
-- ROW_FORMAT=COMPRESSED
-- KEY_BLOCK_SIZE=8;
-- 

#DROP TABLE FILE_HASH_HEAD;-- 
-- CREATE TABLE FILE_HASH_HEAD (
-- 	REPO_OWNER 	VARCHAR(255) NOT NULL,
-- 	REPOSITORY 	VARCHAR(255) NOT NULL,
--     BRANCH 			VARCHAR(255) NOT NULL,
-- 	GIT_PATH 			VARCHAR(255) NOT NULL,
--     HASH_CODE 		VARCHAR(255) NOT NULL,
--     CHECKOUT_ID 	VARCHAR(255),
--     RESULT 				VARCHAR(255),
--     COMPLETED 		BOOLEAN DEFAULT FALSE,
--     PRIMARY KEY (REPO_OWNER, REPOSITORY, BRANCH, GIT_PATH),
--     INDEX IDX_FILE_HASH_HEAD_HASH_CODE (HASH_CODE),
--     INDEX IDX_FILE_HASH_HEAD_COMPLETED (COMPLETED)
-- )
-- ROW_FORMAT=COMPRESSED
-- KEY_BLOCK_SIZE=8;
-- 
-- ALTER TABLE FILE_HASH_HEAD ADD (CHECKOUT_ID 	VARCHAR(255), RESULT  VARCHAR(255), COMPLETED BOOLEAN DEFAULT FALSE);
-- ALTER TABLE FILE_HASH_HEAD ADD INDEX IDX_FILE_HASH_HEAD_HASH_CODE(HASH_CODE);
-- ALTER TABLE FILE_HASH_HEAD ADD INDEX IDX_REPOS_QUEUE_COMPLETED (COMPLETED);

ALTER TABLE `FILE_HASH_HISTORY` DISABLE KEYS;

-- ALTER TABLE FILE_HASH_HISTORY ADD (IS_BUG_FIX	BOOLEAN DEFAULT FALSE);
-- ALTER TABLE FILE_HASH_HISTORY ADD INDEX IDX_FILE_HASH_HISTORY_COMPLETED (COMPLETED);
-- ALTER TABLE FILE_HASH_HISTORY ADD INDEX IDX_FILE_HASH_HISTORY_HASH_CODE (HASH_CODE);
-- 
#DROP TABLE FILE_HASH_HISTORY;
CREATE TABLE FILE_HASH_HISTORY (
	REPO_OWNER 	VARCHAR(100) NOT NULL,
	REPOSITORY 	VARCHAR(100) NOT NULL,
    BRANCH 			VARCHAR(100) NOT NULL,
	GIT_PATH 			VARCHAR(255) NOT NULL,
    HASH_CODE 		VARCHAR(50)   ,
    BYTE_SIZE	        LONG, 
    COMMIT_ID 		VARCHAR(50)   NOT NULL,
    COMMIT_TIME 	LONG                 NOT NULL,
    MESSAGE	        VARCHAR(255),
    IS_BUG_FIX		BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (REPO_OWNER, REPOSITORY, BRANCH, GIT_PATH, COMMIT_ID),
    INDEX IDX_FILE_HASH_HISTORY_HASH_CODE (HASH_CODE)
)
ROW_FORMAT=COMPRESSED
KEY_BLOCK_SIZE=8;

-- 
-- SET SQL_SAFE_UPDATES = 0;
-- UPDATE REPOS_QUEUE 
-- SET COMPLETED = TRUE,
-- 		CHECKOUT_ID = NULL
-- WHERE CHECKOUT_ID = 'dd';
-- 
-- 
-- SELECT REPOSITORY + ":" + BRANCH AS REPOS
-- FROM REPOS_QUEUE
-- WHERE COMPLETED = FALSE
-- 	  AND CHECKOUT_ID IS NULL;
--       
-- 
-- 
-- SELECT * FROM REPOS_QUEUE;
#TRUNCATE TABLE REPOS_QUEUE;