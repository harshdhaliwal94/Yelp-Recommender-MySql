alter table business drop primary key;
alter table business add primary key(business_id);
create index latitude on business(latitude);
create index longitude on business(longitude);
create index city on business(city);

alter table review drop primary key;
alter table review add primary key(review_id);
create index user_id on review(user_id);
create index stars on review(stars);
create index date_review on review(date);
create index b_u_ind on review(business_id,user_id);
create index business_id on review(business_id);

alter table user drop primary key;
alter table user add primary key(user_id);
create index review_count on user(review_count);
create index yelping_since on user(yelping_since);