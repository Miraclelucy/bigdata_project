# hbase create table
create 'streaming_word_count_incr','w','c'
create 'streaming_word_count','w','c'

# mysql start
sudo service mysqld start

# create table
create table streaming_word_count
(
uid VARCHAR(50) NOT NULL,
count VARCHAR(16) NOT NULL
)character set utf8;