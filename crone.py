from crontab import CronTab

cron = CronTab(user='username')

# job1 = cron.new(command='python example1.py')
# job1.minute.every(2)

job2 = cron.new(command='python example1.py')  
job2.second.every(5)


for item in cron:  
    print(item)

cron.write()