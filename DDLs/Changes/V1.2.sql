--Create the session table
create table hue.session(
	session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_nm varchar(32) NOT NULL,
    cre_ts TIMESTAMP(6) DEFAULT NOW()
);

--WARNING: Any existing passwords will cease to work. This was done to drastically
--         Improve security in Hue, In order to work around this for each user you 
--         have in your instance:
--
--         1. Register a new user with password of choice
--         2. Copy the pass_tx and salt_tx values to the original user
--         3. Delete the new user

--Change the password TX to bytea
ALTER TABLE hue."user" ALTER COLUMN pass_tx TYPE bytea USING pass_tx::bytea;

--Add the salt_tx column
ALTER TABLE hue."user" ADD salt_tx bytea NULL;
