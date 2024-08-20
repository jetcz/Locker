
using LockerNamespace;
using System.Data.SqlClient;

namespace SangTests
{
	/// <summary>
	/// Locker test
	/// </summary>
	[TestClass]
	public class LockerTest
	{
		/// <summary>
		/// Test running methods in parallel
		/// </summary>
		[TestMethod]
		public void Test()
		{
			int DOP = new Random().Next(2, 10); //random number from 2 to 10

			TestParallelRuns(() => Task1(), DOP);
			TestParallelRuns(() => Task2(), DOP);
			TestParallelRuns(() => Task3(), DOP);
			TestParallelRuns(() => Task4(), DOP);
			TestParallelRuns(() => Task5(), DOP);
			TestParallelRuns(() => Task6(), DOP);
			TestParallelRuns(() => Task7(), DOP);
		}

		/// <summary>
		/// Run the action in N parallel instances, the locker should ensure that only one instance will actually run, the other should throw exception
		/// </summary>
		/// <param name="a">action</param>
		/// <param name="DOP">how many tasks in parallel</param>
		private void TestParallelRuns(Action a, int DOP)
		{
			int errCounter = 0;

			List<Task> tasks = new(DOP);
			for (int i = 0; i < DOP; i++)
			{
				tasks.Add(Task.Run(a));
			}

			try
			{
				Task.WaitAll([.. tasks]);
			}
			catch (AggregateException ex)
			{
				if (ex.InnerExceptions.Any(x => x is not Locker.LockException))
				{
					throw;
				}

				errCounter = ex.InnerExceptions.Count(x => x is Locker.LockException);
			}

			Assert.AreEqual(DOP - 1, errCounter); //only one task should have finished succesfully
		}

		class Task1Lock : ILockable
		{
			public string ResourceID => "SimpleTask123456789qwertzuiopasdfghjklyxcvbnm";

			public string ResourceName => "Simple task with own connection";
		}

		private void Task1()
		{
			Task1Lock resource = new();

			using (Locker locker = new(resource))
			{
				//do your thing in this block

				Task.Delay(100).Wait();
				Assert.IsTrue(locker.CheckLock());
			}
		}

		class Task2Lock : ILockable
		{
			public string ResourceID => "SimpleTask2";

			public string ResourceName => "Simple task with supplied connection";
		}

		private void Task2()
		{
			Task2Lock resource = new();


			SqlCommand cmd = new()
			{
				CommandText = @"
                    select kokos = 1 into #t
                    WAITFOR DELAY '00:00:00.100';
                    select * from #t                
                    drop table #t"
			};

			using (cmd.Connection = DB.Connect())
			using (Locker locker = new(resource, cmd.Connection)) //lock must be before execute
			using (SqlDataReader r = cmd.ExecuteReader())
			{
				while (r.Read())
				{

				}

				Assert.IsTrue(locker.CheckLock());
			}
		}

		class Task3Lock : ILockable
		{
			public string ResourceID => "SimpleTask3";

			public string ResourceName => "Simple task with own connection";
		}

		private void Task3()
		{
			Task3Lock resource = new();

			using (Locker locker = new(resource)) //this will create and hold its own connection to manage the lock
			{
				using (SqlConnection c = DB.Connect())
				using (SqlTransaction t = c.BeginTransaction())
				{
					Part1(c, t);
					Part2(c, t);
					Part3(c, t);

					t.Commit();

					Assert.IsTrue(locker.CheckLock());
				}

				Assert.IsTrue(locker.CheckLock());
			}
		}

		class Task4Lock : ILockable
		{
			public string ResourceID => "SimpleTask4";

			public string ResourceName => "Simple task with supplied connection and transaction";
		}

		private void Task4()
		{
			Task4Lock resource = new();

			using (SqlConnection c = DB.Connect())
			using (SqlTransaction t = c.BeginTransaction())
			using (Locker locker = new(resource, c, t)) //the lock will be scoped to this transaction
			{
				Part1(c, t);
				Part2(c, t);
				Part3(c, t);

				Assert.IsTrue(locker.CheckLock());

				t.Commit();
			}
		}

		class Task5Lock : ILockable
		{
			public string ResourceID => "SimpleTask5";

			public string ResourceName => "Simple task without using";
		}

		//do not use this approach, this is here just for completeness
		private void Task5()
		{
			Task5Lock resource = new();

			Locker locker = new(resource);

			using (SqlConnection c = DB.Connect())
			using (SqlTransaction t = c.BeginTransaction())
			{
				Part1(c, t);
				Part2(c, t);
				Part3(c, t);
				t.Commit();

				Assert.IsTrue(locker.CheckLock());
			}

			Assert.IsTrue(locker.CheckLock());

			locker.Unlock(); //not really needed

			Assert.IsFalse(locker.CheckLock());

			locker.Dispose(); //this will also unlock
		}

		class Task6Lock : ILockable
		{
			public string ResourceID => "SimpleTask6";

			public string ResourceName => "Simple task with supplied connection and transaction but with dispose being earlier than close of connection";
		}

		private void Task6()
		{
			Task6Lock resource = new();

			using (SqlConnection c = DB.Connect())
			using (SqlTransaction t = c.BeginTransaction())
			{
				using (Locker locker = new(resource, c, t)) //the lock will be scoped to this transaction
				{
					Assert.IsTrue(locker.CheckLock());

					Part1(c, t);
					Part2(c, t);
					Part3(c, t);

					Assert.IsTrue(locker.CheckLock());
				}

				//do some other shit, at this point, the lock should have been released    
				t.Commit();
			}
		}

		class Task7Lock : ILockable
		{
			public string ResourceID => "SimpleTask7";

			public string ResourceName => "Simple task with supplied connection but with dispose being earlier than close of connection";
		}

		private void Task7()
		{
			Task7Lock resource = new();

			using (SqlConnection c = DB.Connect())
			{
				using (Locker locker = new(resource, c)) //the lock will be scoped to this connection
				{
					Assert.IsTrue(locker.CheckLock());

					Part1(c, null);
					Part2(c, null);
					Part3(c, null);

					Assert.IsTrue(locker.CheckLock());
				}

				//do some other shit, at this point, the lock should have been released               
			}
		}

		private void Part1(SqlConnection c, SqlTransaction? t)
		{
			SqlCommand cmd = new()
			{
				CommandText = @"
                    select kokos = 1 into #t 
                    WAITFOR DELAY '00:00:00.100'",
				Connection = c,
				Transaction = t
			};

			cmd.ExecuteNonQuery();
		}
		private void Part2(SqlConnection c, SqlTransaction? t)
		{
			SqlCommand cmd = new()
			{
				CommandText = @"
                    update #t set kokos = 2",
				Connection = c,
				Transaction = t
			};

			cmd.ExecuteNonQuery();
		}
		private void Part3(SqlConnection c, SqlTransaction? t)
		{
			SqlCommand cmd = new()
			{
				CommandText = @"
                    drop table #t",
				Connection = c,
				Transaction = t
			};

			cmd.ExecuteNonQuery();
		}

		/// <summary>
		/// Test multiple check lock
		/// </summary>
		[TestMethod]
		public void TestMultipleCheckLock()
		{
			Locker.CheckLock(new Task1Lock(), new Task2Lock(), new Task2Lock());
		}

		/// <summary>
		/// Test embedded tran inside of cmd
		/// </summary>
		[TestMethod]
		public void TestEmbeddedTransaction()
		{
			SqlCommand cmdCreateProc = new()
			{
				CommandText = @"
                create or alter procedure dbo.testdummy
                as
                begin
                    set nocount on
	                set xact_abort on
	                begin tran

	                select 1

	                commit
                end
                "
			};

			cmdCreateProc.ExecuteNonQuery();

			SqlCommand cmdCreateProc2 = new()
			{
				CommandText = @"
                create or alter procedure dbo.testdummy2
                as
                begin
                    set nocount on
	                set xact_abort on
	                begin tran

	    			--RAISERROR('KOKOS', 11, 1)
			        --RETURN

	                commit
                    set xact_abort off
                end
                "
			};

			cmdCreateProc2.ExecuteNonQuery();


			Task1Lock resource = new();

			using (SqlConnection c = DB.Connect())
			using (Locker locker = new(resource, c))
			{
				SqlCommand cmd1 = new()
				{
					Connection = c,
					CommandText = "testdummy",
					CommandType = System.Data.CommandType.StoredProcedure
				};
				cmd1.ExecuteNonQuery();

				SqlCommand cmd2 = new()
				{
					Connection = c,
					CommandText = "testdummy2",
					CommandType = System.Data.CommandType.StoredProcedure
				};
				cmd2.ExecuteNonQuery();
			}
		}
	}
}
