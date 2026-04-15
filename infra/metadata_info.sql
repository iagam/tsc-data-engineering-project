TRUNCATE TABLE `devx-tsc.sample_dataset_5.metadata_info`;

INSERT INTO `devx-tsc.sample_dataset_5.metadata_info`
(table_name, column_name, data_type, is_nullable, source, created_at)

VALUES

-- ================= USERS_USER =================
('users_user','user_id','STRING',FALSE,'randomuser_api > $.login.uuid',CURRENT_TIMESTAMP()),
('users_user','title','STRING',TRUE,'randomuser_api > $.name.title',CURRENT_TIMESTAMP()),
('users_user','gender','STRING',TRUE,'randomuser_api > $.gender',CURRENT_TIMESTAMP()),
('users_user','first_name','STRING',TRUE,'randomuser_api > $.name.first',CURRENT_TIMESTAMP()),
('users_user','last_name','STRING',TRUE,'randomuser_api > $.name.last',CURRENT_TIMESTAMP()),
('users_user','dob','TIMESTAMP',TRUE,'randomuser_api > $.dob.date',CURRENT_TIMESTAMP()),
('users_user','age','INT64',TRUE,'randomuser_api > $.dob.age',CURRENT_TIMESTAMP()),
('users_user','nationality','STRING',TRUE,'randomuser_api > $.nat',CURRENT_TIMESTAMP()),
('users_user','ingested_at','TIMESTAMP',FALSE,'system',CURRENT_TIMESTAMP()),

-- ================= USERS_LOGIN =================
('users_login','user_id','STRING',FALSE,'randomuser_api > $.login.uuid',CURRENT_TIMESTAMP()),
('users_login','username','STRING',TRUE,'randomuser_api > $.login.username',CURRENT_TIMESTAMP()),
('users_login','password_hash','STRING',TRUE,'randomuser_api > $.login.sha256',CURRENT_TIMESTAMP()),
('users_login','registered_date','TIMESTAMP',TRUE,'randomuser_api > $.registered.date',CURRENT_TIMESTAMP()),
('users_login','ingested_at','TIMESTAMP',FALSE,'system',CURRENT_TIMESTAMP()),

-- ================= USERS_LOCATION =================
('users_location','user_id','STRING',FALSE,'randomuser_api > $.login.uuid',CURRENT_TIMESTAMP()),
('users_location','street_number','INTEGER',TRUE,'randomuser_api > $.location.street.number',CURRENT_TIMESTAMP()),
('users_location','street_name','STRING',TRUE,'randomuser_api > $.location.street.name',CURRENT_TIMESTAMP()),
('users_location','city','STRING',TRUE,'randomuser_api > $.location.city',CURRENT_TIMESTAMP()),
('users_location','state','STRING',TRUE,'randomuser_api > $.location.state',CURRENT_TIMESTAMP()),
('users_location','country','STRING',TRUE,'randomuser_api > $.location.country',CURRENT_TIMESTAMP()),
('users_location','postcode','STRING',TRUE,'randomuser_api > $.location.postcode',CURRENT_TIMESTAMP()),
('users_location','latitude','FLOAT',TRUE,'randomuser_api > $.location.coordinates.latitude',CURRENT_TIMESTAMP()),
('users_location','longitude','FLOAT',TRUE,'randomuser_api > $.location.coordinates.longitude',CURRENT_TIMESTAMP()),
('users_location','ingested_at','TIMESTAMP',FALSE,'system',CURRENT_TIMESTAMP()),

-- ================= USERS_CONTACT =================
('users_contact','user_id','STRING',FALSE,'randomuser_api > $.login.uuid',CURRENT_TIMESTAMP()),
('users_contact','email','STRING',TRUE,'randomuser_api > $.email',CURRENT_TIMESTAMP()),
('users_contact','phone','STRING',TRUE,'randomuser_api > $.phone',CURRENT_TIMESTAMP()),
('users_contact','cell','STRING',TRUE,'randomuser_api > $.cell',CURRENT_TIMESTAMP()),
('users_contact','ingested_at','TIMESTAMP',FALSE,'system',CURRENT_TIMESTAMP()),

-- ================= USERS_ASSETS =================
('users_assets','user_id','STRING',FALSE,'randomuser_api > $.login.uuid',CURRENT_TIMESTAMP()),
('users_assets','picture_large','STRING',TRUE,'randomuser_api > $.picture.large',CURRENT_TIMESTAMP()),
('users_assets','picture_medium','STRING',TRUE,'randomuser_api > $.picture.medium',CURRENT_TIMESTAMP()),
('users_assets','picture_thumbnail','STRING',TRUE,'randomuser_api > $.picture.thumbnail',CURRENT_TIMESTAMP()),
('users_assets','ingested_at','TIMESTAMP',TRUE,'system',CURRENT_TIMESTAMP());