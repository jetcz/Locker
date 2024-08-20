# Locker
Simply lock your code with a using statement to prevent parallel execution!

				using (new Locker(new MyLock(MyResourceID)))
				{
					StartLongRunningTask(MyResourceID); //we don't want anyone else to run this task for given MyResourceID at the same time
				}

  It works on sql level and requires MS SQL server. The assumption is that there is ONLY ONE DB server. It will probably not work if there are multiple database servers... It can however work if there are multiple app servers.
