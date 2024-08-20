using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace LockerNamespace
{
	/// <summary>
	/// Definitions of lockable resource
	/// </summary>
	public interface ILockable
	{
		/// <summary>
		/// Unique identifier of the task type, e.g. ctrrecalc1234 or reconcilliation53125
		/// </summary>
		string ResourceID { get; }

		/// <summary>
		/// Display name of the resource, e.g. Contract recalculation ID 123
		/// </summary>
		string ResourceName { get; }
	}

	/// <summary>
	/// Use this bad boy if you want to limit execution of some code to only single instance, ie. prevents running enclosed code in parallel.
	/// To be used with using statement.
	/// Internally it uses DB sp_getapplock and DB connection to scope the lock.
	/// See LockerTest.cs for examples.
	/// </summary>
	public class Locker : IDisposable
	{
		/// <summary>
		/// Lock result
		/// </summary>
		public enum LockResult
		{
			//https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver15

			/// <summary>
			/// The lock was successfully granted synchronously.
			/// </summary>
			Success = 0,

			/// <summary>
			/// The lock was granted successfully after waiting for other incompatible locks to be released.
			/// </summary>
			SuccessAfterWait = 1,

			/// <summary>
			/// The lock request timed out.
			/// </summary>
			TimedOut = -1,

			/// <summary>
			/// The lock request was canceled.
			/// </summary>
			CanceledByCaller = -2,

			/// <summary>
			/// The lock request was chosen as a deadlock victim.
			/// </summary>
			ChosenAsDeadlockVictim = -3,

			/// <summary>
			/// Indicates a parameter validation or other call error.
			/// </summary>
			OtherError = 999
		}

		/// <summary>
		/// Lock exception when lock cannot be obtained
		/// </summary>
		public class LockException : Exception
		{
			/// <summary>
			/// Result of sp_getapplock
			/// </summary>
			public LockResult LockResult { get; private set; }

			/// <summary>
			/// Constructor
			/// </summary>
			/// <param name="resource"></param>
			/// <param name="result"></param>
			public LockException(ILockable resource, LockResult result)
				: base($"Cannot obtain lock on \"{(string.IsNullOrEmpty(resource.ResourceName) ? resource.ResourceID : resource.ResourceName)}\". Another process may be using it.")
			{
				LockResult = result;
			}
		}

		private readonly ILockable Resource;
		private readonly SqlConnection c;
		private readonly SqlTransaction t;
		private readonly bool OwnConnection;
		private bool Disposed = false;

		/// <summary>
		/// Constructor
		/// To be used with using statement!
		/// </summary>
		/// <param name="resource">Unique identifier of the task</param>
		/// <param name="lockTimeout">Milliseconds; 0 = return immediately, -1 = wait forever</param>
		public Locker(ILockable resource, int lockTimeout = 0) : this(resource, null, null, lockTimeout) { }

		/// <summary>
		/// Constructor
		/// Use this if all of the code you want to lock is inside single connection
		/// </summary>
		/// <param name="resource">Unique identifier of the task</param>
		/// <param name="c">Scope of this connection will also be scope of the lock</param>
		/// <param name="lockTimeout">Milliseconds; 0 = return immediately, -1 = wait forever</param>
		public Locker(ILockable resource, SqlConnection c, int lockTimeout = 0) : this(resource, c, null, lockTimeout) { }

		/// <summary>
		/// Constructor
		/// Use this if all of the code you want to lock is inside single connection and transaction
		/// </summary>
		/// <param name="resource">Unique identifier of the task</param>
		/// <param name="c">Scope of this connection will also be scope of the lock</param>
		/// <param name="t"></param>
		/// <param name="lockTimeout">Milliseconds; 0 = return immediately, -1 = wait forever</param>
		/// <exception cref="ArgumentNullException"></exception>
		/// <exception cref="Exceptions.AdPointException">If not possible to get the lock, ie. same task is already running</exception>
		public Locker(ILockable resource, SqlConnection c, SqlTransaction t, int lockTimeout = 0)
		{
			if (string.IsNullOrEmpty(resource?.ResourceID))
			{
				throw new ArgumentNullException(nameof(resource));
			}

			Resource = resource;

			if (c is null)
			{
				this.c = DB.Connect();
				this.t = null;
				OwnConnection = true;
			}
			else
			{
				this.c = c;
				this.t = t;
				OwnConnection = false;
			}

			LockResult result = Lock(lockTimeout);

			switch (result)
			{
				case LockResult.Success:
				case LockResult.SuccessAfterWait:
					return;
				default:
					throw new LockException(resource, result);
			}
		}

		/// <summary>
		/// Lock resource
		/// </summary>
		/// <param name="lockTimeout">milliseconds; 0 = return immediately, -1 = wait forever</param>
		/// <returns></returns>
		private LockResult Lock(int lockTimeout)
		{
			SqlCommand cmd = new SqlCommand
			{
				Connection = c,
				Transaction = t,
				CommandText = @"sp_getapplock",
				CommandType = CommandType.StoredProcedure
			};

			if (lockTimeout > cmd.CommandTimeout)
			{
				cmd.CommandTimeout = lockTimeout + 5; //we don't want the command to expire sooner than the limit for obtaining the lock
			}

			cmd.Parameters.AddWithValue("@Resource", Resource.ResourceID);
			cmd.Parameters.AddWithValue("@LockMode", "Exclusive");
			cmd.Parameters.AddWithValue("@LockOwner", t is null ? "Session" : "Transaction");
			cmd.Parameters.AddWithValue("@LockTimeout", lockTimeout);

			SqlParameter res = cmd.Parameters.Add("@Res", SqlDbType.Int);
			res.Direction = ParameterDirection.ReturnValue;

			cmd.ExecuteNonQuery();

			return (LockResult)(int)res.Value;
		}

		/// <summary>
		/// Manually unlock resource.
		/// The resource is automatically unlocked when session expires (cnn close), usually no need to call this.
		/// This does NOT close the underlying connection
		/// </summary>
		/// <returns></returns>
		public bool Unlock()
		{
			SqlCommand cmd = new SqlCommand
			{
				Connection = c,
				Transaction = t,
				CommandText = @"sp_releaseapplock",
				CommandType = CommandType.StoredProcedure
			};

			cmd.Parameters.AddWithValue("@Resource", Resource.ResourceID);
			cmd.Parameters.AddWithValue("@LockOwner", t is null ? "Session" : "Transaction");

			SqlParameter res = cmd.Parameters.Add("@Res", SqlDbType.Int);
			res.Direction = ParameterDirection.ReturnValue;

			cmd.ExecuteNonQuery();

			return (int)res.Value == (int)LockResult.Success;
		}

		/// <summary>
		/// Check for lock
		/// </summary>
		/// <returns>true = resource locked; false = resource not locked</returns>
		public bool CheckLock()
		{
			return CheckLock(Resource);
		}

		/// <summary>
		/// Check for lock
		/// </summary>
		/// <param name="resources">Unique identifier of the task type, e.g. ctrrecalc1234 or reconcilliation53125 or InvoicingOfAds etc.</param>
		/// <returns>true = at least one of the resources is locked; false = none of the resources is locked</returns>
		public static bool CheckLock(params ILockable[] resources)
		{
			List<string> resourceIds = resources?
				.Where(x => x != null && !string.IsNullOrEmpty(x.ResourceID))
				.Select(x => x.ResourceID)
				.ToList();

			if (!resourceIds?.Any() ?? true)
			{
				throw new ArgumentException($"{nameof(resources)} is null or empty");
			}

			SqlCommand cmd = new SqlCommand();
			StringBuilder declarations = new StringBuilder();
			StringBuilder parameters = new StringBuilder();

			for (int i = 0; i < resourceIds.Count; i++)
			{
				//applock_test returns 1 if lock can be obtained, 0 if lock already in place
				declarations.AppendFormat($@"
                    declare @p_{i} bit = (select applock_test(@DbPrincipal, @ResourceID_{i}, @LockMode, @LockOwner))");

				cmd.Parameters.AddWithValue($"@ResourceID_{i}", resourceIds[i]);

				// ~ means invert
				// build this expression: select ~@p_0 | ~@p_1 | ~@p_2  
				parameters.AppendFormat("~@p_{0}{1}"
					, i
					, i < resourceIds.Count - 1 ? " | " : "");
			}

			cmd.CommandText = $@"
                {declarations}
                select {parameters}
            ";
			cmd.Parameters.AddWithValue("@DbPrincipal", "public");
			cmd.Parameters.AddWithValue("@LockMode", "Exclusive");
			cmd.Parameters.AddWithValue("@LockOwner", "Session"); //this will work just fine even if the lock was scoped to tran

			using (cmd.Connection = DB.Connect())
			{
				return (bool)cmd.ExecuteScalar();
			}
		}

		/// <summary>
		/// Closes DB connection if it was created in constructor, this releases the lock
		/// </summary>
		public void Dispose()
		{
			if (Disposed)
			{
				return;
			}

			if (OwnConnection)
			{
				c?.Close();
				Disposed = true;
			}
			else if (c?.State == ConnectionState.Open
				&& (t is null || t?.Connection?.State == ConnectionState.Open))
			{
				Disposed = Unlock();
			}
		}
	}
}