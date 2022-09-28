# CMT hints
## Common problems / recommendations
Problem with validation connection of CCV1 DB
Authentication problem - after a few bad login attempts you might lock your DB user and you have to talk to DBA to unlock it.
Authentication problem - sometimes DBA generates a password with a # sign and it is being read as a comment mark and all signs after it is not being read. I have tried to escape that character but couldn't so I think the best way is to talk do DBA to regenerate the password
Missing schema at connection string. By default, Hana tries to use schema from a user so it would dbmigration but this schema is empty and you should add to connection string parameter currentSchema\=${migration.ds.source.db.schema}  to login into correct schema. If you don't know your schema usually it is 3 letters of customer code + environment code for example for Distrelec s1 schema would be dats but sometimes there is also a number in the end so it might be datd2.
Timeout problem - you should check if you using NAT IP usually it is starting from 100. If you using the proper IP there might be also missing NATing on the CCV2 side or some problems with the VPN.
Login out from HAC during migration. I recommend using a separate browser for migration and don't click anything on this browser and it shouldn't log out because even if you have a separate tab for that browser can suspend this tab and you will be logged out.
You can add a property log4j2.logger.migrationToolkit.level=Debug so you will see all progress also at kibana.
Increasing batchsize
Scaling up backoffice aspect
Shut down all aspects except backoffice. If you got running hotfolders it might process some data during that time and it might use initialized type system and might break the type system.
Add properties for dropping indexes. It might help if you have some duplicates in DB.
* migration.data.indices.drop.enabled=true
* migration.data.indices.disable.enabled=true

During migration also faced one problem and I saw at logs migration stalled. Probably this might be some CMT bug because the migration process was working properly. I fixed that problem adding property migration.stalled.timeout=20000 by default it is 2 h
It is good to check how much RAM Hana is using before migration because sometimes it can cause random problems and migration might be interrupted and it is worth restarting Hana.
## Performance
During our Distrelec project, we made several migrations with different configs, and finally, we have achieved over 3 times better performance.

For all migration, we used 4000 DTU

Our first migration was before DB refresh and It took over 9h which for 70 GB DB on Azure is a very bad time. It was performed using the default project.properties parameters and default scaling of backoffice aspect (2 CPU 6 GB ram 2 pods). In this run, our migration didn't use so much DTU which in the old ADF approach wasn't a problem.

Our next migration was after refresh from production and DB was much bigger it has 120 GB on Azure after migration and took only 3:30 h. We changed at this run a few parameters such as:

* migration.data.workers.reader.maxtasks=10
* migration.data.workers.writer.maxtasks=20
* migration.data.reader.batchsize=4000
  We notice that HANA has better performance with queries with bigger results than small ones I think the default is 1000.

We have also scaled up backoffice to 6 CPU 8 GB RAM and left only 1 pod and as a result, we get almost 3 times better performance. I think 6 CPU and 8 GB RAM is the maximum that we can give as a migration team with more CPU and RAM pods weren't stable in my case. At this run, we used a lot more DTU, and a few times we reached 100% usage so performance was much better.

Our last run was with a similar DB (120 GB) that has failed because of a lack of memory on the pod. So we made a ticket to scale up backoffice aspect to max values l1 + support can give and we get 8 Cores 16 GB RAM. This run also give us 10 min better results and migration was stable.



My recommendation is to always scale up backoffice at least to the maximum we can get at model-t 12 CPU 20 GB RAM and for bigger DB request for more resources for migration time and scale down after migration. I see there is a huge usage of CPU and RAM during CMT migration and has a very big impact on performance. Also, you can try increasing batchsize.

What you can do after the first migration is to check the times of migration of all tables and check with the customer if you can clean up tables that were copied for a long time with some retention time.
