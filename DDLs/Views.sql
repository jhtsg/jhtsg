-- hue.artist_statistics source

CREATE OR REPLACE VIEW hue.artist_statistics
AS SELECT co.user_nm,
    co.artist_id,
    ar.artist_nm,
    count(*) AS comm_cnt,
    sum(co.comm_price_nb) AS spent_nb,
    max(co.pblsh_dt) AS last_pblsh_dt,
    avg(co.comm_ttc_nb) AS avg_ttc_nb,
    mode() WITHIN GROUP (ORDER BY co.comm_type_cd) AS most_common_type_cd,
    ar.artist_img_present_in AS image_in
   FROM hue.artist ar,
    hue.comm co
  WHERE ar.artist_id = co.artist_id
  GROUP BY co.user_nm, co.artist_id, ar.artist_nm, ar.artist_img_present_in;


-- hue.at_a_glance_view source

CREATE OR REPLACE VIEW hue.at_a_glance_view
AS SELECT user_nm,
    count(
        CASE
            WHEN comm_started_in THEN 1
            ELSE NULL::integer
        END) AS total_comm_nb,
    sum(
        CASE
            WHEN comm_started_in THEN comm_price_nb
            ELSE 0
        END) AS total_spent_nb,
    count(
        CASE
            WHEN NOT comm_started_in THEN 1
            ELSE NULL::integer
        END) AS total_not_comm_nb,
    sum(
        CASE
            WHEN NOT comm_started_in THEN comm_price_nb
            ELSE 0
        END) AS total_not_spent_nb,
    avg(comm_price_nb) AS avg_price_nb,
    avg(
        CASE
            WHEN comm_ttc_nb IS NOT NULL THEN comm_ttc_nb
            ELSE NULL::integer
        END) AS avg_ttc_nb,
    sum(
        CASE
            WHEN comm_started_in THEN comm_price_nb
            ELSE 0
        END) / NULLIF(count(DISTINCT
        CASE
            WHEN comm_started_in THEN date_trunc('month'::text, COALESCE(start_dt, cre_ts::date)::timestamp with time zone)
            ELSE NULL::timestamp with time zone
        END), 0) AS avg_spent_nb,
    count(*) / NULLIF(count(DISTINCT date_trunc('month'::text, COALESCE(start_dt, cre_ts::date)::timestamp with time zone)), 0) AS avg_comm_by_month_nb
   FROM hue.comm
  GROUP BY user_nm;


-- hue.char_statistics source

CREATE OR REPLACE VIEW hue.char_statistics
AS SELECT co.user_nm,
    ch.char_id,
    ch.char_nm,
    ch.char_color_tx,
    count(*) AS comm_cnt,
    sum(co.comm_price_nb) AS spent_nb,
    max(co.pblsh_dt) AS last_pblsh_dt,
    ch.char_img_present_in AS image_in
   FROM hue."char" ch,
    hue.comm co,
    hue.comm_char_map cm
  WHERE ch.char_id = cm.char_id AND cm.comm_id = co.comm_id
  GROUP BY co.user_nm, ch.char_id, ch.char_nm, ch.char_color_tx, ch.char_img_present_in;


-- hue.monthly_comm_price_cat_view source

CREATE OR REPLACE VIEW hue.monthly_comm_price_cat_view
AS SELECT user_nm,
    comm_year_nb,
    comm_month_nb,
    count(
        CASE
            WHEN comm_price_nb < 50 THEN 1
            ELSE NULL::integer
        END) AS small_comm_cnt,
    count(
        CASE
            WHEN comm_price_nb >= 50 AND comm_price_nb < 80 THEN 1
            ELSE NULL::integer
        END) AS med_comm_cnt,
    count(
        CASE
            WHEN comm_price_nb >= 80 THEN 1
            ELSE NULL::integer
        END) AS large_comm_cnt
   FROM hue.comm
  GROUP BY user_nm, comm_year_nb, comm_month_nb
  ORDER BY comm_year_nb, comm_month_nb;


-- hue.monthly_spend_view source

CREATE OR REPLACE VIEW hue.monthly_spend_view
AS SELECT user_nm,
    comm_year_nb,
    comm_month_nb,
    sum(
        CASE
            WHEN comm_started_in THEN comm_price_nb
            ELSE 0
        END) AS confirmed_spent_nb,
    sum(
        CASE
            WHEN NOT comm_started_in THEN comm_price_nb
            ELSE 0
        END) AS potential_spent_nb
   FROM hue.comm
  GROUP BY user_nm, comm_year_nb, comm_month_nb
  ORDER BY comm_year_nb, comm_month_nb;


-- hue.monthly_status_view source

CREATE OR REPLACE VIEW hue.monthly_status_view
AS SELECT user_nm,
    comm_year_nb,
    comm_month_nb,
    count(
        CASE
            WHEN comm_status_cd = 0 THEN 1
            ELSE NULL::integer
        END) AS brainstorm_cnt,
    count(
        CASE
            WHEN comm_status_cd = 1 THEN 1
            ELSE NULL::integer
        END) AS scheduled_cnt,
    count(
        CASE
            WHEN comm_status_cd = 2 THEN 1
            ELSE NULL::integer
        END) AS in_prog_cnt,
    count(
        CASE
            WHEN comm_status_cd = 3 THEN 1
            ELSE NULL::integer
        END) AS done_cnt,
    count(
        CASE
            WHEN comm_status_cd = 4 THEN 1
            ELSE NULL::integer
        END) AS publish_cnt
   FROM hue.comm
  GROUP BY user_nm, comm_year_nb, comm_month_nb
  ORDER BY comm_year_nb, comm_month_nb;


-- hue.overdue_comms source

CREATE OR REPLACE VIEW hue.overdue_comms
AS SELECT co.user_nm,
    co.comm_id,
    co.comm_nm,
    co.comm_status_cd,
    ceil((now()::date - co.start_dt)::numeric - GREATEST(astats.avg_ttc_nb * 1.25, 2.0))::integer AS overdue_days_nb
   FROM hue.comm co,
    hue.artist_statistics astats
  WHERE co.comm_status_cd = 2 AND co.start_dt IS NOT NULL AND co.artist_id = astats.artist_id AND (now()::date - co.start_dt)::numeric > GREATEST(astats.avg_ttc_nb * 1.25, 2.0) AND astats.avg_ttc_nb IS NOT NULL
UNION
 SELECT comm.user_nm,
    comm.comm_id,
    comm.comm_nm,
    comm.comm_status_cd,
    now()::date - comm.done_dt AS overdue_days_nb
   FROM hue.comm
  WHERE comm.comm_status_cd = 3 AND comm.done_dt IS NOT NULL AND (now()::date - comm.done_dt) > 7;


-- hue.tag_statistics source

CREATE OR REPLACE VIEW hue.tag_statistics
AS SELECT co.user_nm,
    cm.comm_tag_id,
    ct.comm_tag_nm,
    ct.comm_tag_color_tx,
    count(*) AS comm_cnt,
    sum(co.comm_price_nb) AS spent_nb,
    max(co.pblsh_dt) AS last_pblsh_dt,
    false AS image_in
   FROM hue.comm_tag ct,
    hue.comm co,
    hue.comm_tag_map cm
  WHERE ct.comm_tag_id = cm.comm_tag_id AND cm.comm_id = co.comm_id
  GROUP BY co.user_nm, cm.comm_tag_id, ct.comm_tag_nm, ct.comm_tag_color_tx;


-- hue.yearly_artist_statistics source

CREATE OR REPLACE VIEW hue.yearly_artist_statistics
AS SELECT co.user_nm,
    co.comm_year_nb,
    co.artist_id,
    ar.artist_nm,
    count(*) AS comm_cnt,
    sum(co.comm_price_nb) AS spent_nb,
    max(co.pblsh_dt) AS last_pblsh_dt,
    ar.artist_img_present_in AS image_in
   FROM hue.artist ar,
    hue.comm co
  WHERE ar.artist_id = co.artist_id
  GROUP BY co.user_nm, co.artist_id, co.comm_year_nb, ar.artist_nm, ar.artist_img_present_in;


-- hue.yearly_at_a_glance_view source

CREATE OR REPLACE VIEW hue.yearly_at_a_glance_view
AS SELECT user_nm,
    comm_year_nb,
    count(
        CASE
            WHEN comm_started_in THEN 1
            ELSE NULL::integer
        END) AS total_comm_nb,
    sum(
        CASE
            WHEN comm_started_in THEN comm_price_nb
            ELSE 0
        END) AS total_spent_nb,
    count(
        CASE
            WHEN NOT comm_started_in THEN 1
            ELSE NULL::integer
        END) AS total_not_comm_nb,
    sum(
        CASE
            WHEN NOT comm_started_in THEN comm_price_nb
            ELSE 0
        END) AS total_not_spent_nb,
    avg(comm_price_nb) AS avg_price_nb,
    avg(
        CASE
            WHEN comm_ttc_nb IS NOT NULL THEN comm_ttc_nb
            ELSE NULL::integer
        END) AS avg_ttc_nb,
    sum(
        CASE
            WHEN comm_started_in THEN comm_price_nb
            ELSE 0
        END) / NULLIF(count(DISTINCT
        CASE
            WHEN comm_started_in THEN date_trunc('month'::text, COALESCE(start_dt, cre_ts::date)::timestamp with time zone)
            ELSE NULL::timestamp with time zone
        END), 0) AS avg_spent_nb,
    count(*) / NULLIF(count(DISTINCT date_trunc('month'::text, COALESCE(start_dt, cre_ts::date)::timestamp with time zone)), 0) AS avg_comm_by_month_nb
   FROM hue.comm
  GROUP BY user_nm, comm_year_nb;


-- hue.yearly_char_statistics source

CREATE OR REPLACE VIEW hue.yearly_char_statistics
AS SELECT co.user_nm,
    co.comm_year_nb,
    ch.char_id,
    ch.char_nm,
    ch.char_color_tx,
    count(*) AS comm_cnt,
    sum(co.comm_price_nb) AS spent_nb,
    max(co.pblsh_dt) AS last_pblsh_dt,
    ch.char_img_present_in AS image_in
   FROM hue."char" ch,
    hue.comm co,
    hue.comm_char_map cm
  WHERE ch.char_id = cm.char_id AND cm.comm_id = co.comm_id
  GROUP BY co.user_nm, ch.char_id, co.comm_year_nb, ch.char_nm, ch.char_color_tx, ch.char_img_present_in;


-- hue.yearly_tag_statistics source

CREATE OR REPLACE VIEW hue.yearly_tag_statistics
AS SELECT co.user_nm,
    co.comm_year_nb,
    cm.comm_tag_id,
    ct.comm_tag_nm,
    ct.comm_tag_color_tx,
    count(*) AS comm_cnt,
    sum(co.comm_price_nb) AS spent_nb,
    max(co.pblsh_dt) AS last_pblsh_dt,
    false AS image_in
   FROM hue.comm_tag ct,
    hue.comm co,
    hue.comm_tag_map cm
  WHERE ct.comm_tag_id = cm.comm_tag_id AND cm.comm_id = co.comm_id
  GROUP BY co.user_nm, cm.comm_tag_id, co.comm_year_nb, ct.comm_tag_nm, ct.comm_tag_color_tx;