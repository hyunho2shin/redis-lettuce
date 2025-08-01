
-- shell 자동으로 install이 나와서 설치했음.
cd C:\Users\Administrator\Desktop\info\util\redis-windows-7.2.5.0

-- windows powershell
cat set.sql | .\redis-cli -h redisc-pr54q-fkr.cdb.fin-ntruss.com -p 6379 -n 2 --raw --pipe
