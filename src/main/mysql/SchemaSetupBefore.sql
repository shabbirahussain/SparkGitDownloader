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