using jessielesbian.OpenCEX.oracle;

using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Numerics;

namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils{
		public static readonly ulong derivativesExpiry;
		private static void DerivativesSettlementThread(){
			Queue<ConcurrentJob> waiting = new Queue<ConcurrentJob>();
			Dictionary<string, BigInteger> priceCaches = new Dictionary<string, BigInteger>();
			while (!abort)
			{
				try{
					ulong time = (ulong) DateTimeOffset.Now.ToUnixTimeSeconds();
					using SQLCommandFactory sql = GetSQL(IsolationLevel.ReadCommitted);

					using MySqlDataReader mySqlDataReader = sql.GetCommand("SELECT Name, Expiry FROM Derivatives;").ExecuteReader();
					while (mySqlDataReader.Read())
					{

						ulong expiry = mySqlDataReader.GetUInt64("Expiry");
						if (time > expiry)
						{
							string longname = mySqlDataReader.GetString("Name");
							int pivot = longname.LastIndexOf('_');
							string underlying = longname.Substring(0, pivot);
							if (!priceCaches.TryGetValue(underlying, out BigInteger price))
							{
								{
									CheckSafety(oracles.TryGetValue(underlying, out IOracle oracle), "Missing oracle!");
									try
									{
										price = oracle.GetPriceAt(expiry, int.MaxValue);
									}
									catch
									{
										//BATTLE SHORT this mode of failure
										continue;
									}
									if(price == BigInteger.MinusOne){
										continue;
									}
								}

								CheckSafety(priceCaches.TryAdd(underlying, price), "Unable to cache price!");
							}

							ulong trueExpiry = expiry + derivativesExpiry;
							if (debug)
							{
								//Prevent repeated settlements on dev server due to downtime
								trueExpiry = Math.Max(trueExpiry, time);
								trueExpiry = Math.Max(trueExpiry, time);
							}
							waiting.Enqueue(new DerivativesSettlementJob(longname, longname + "_SHORT", trueExpiry, longname[pivot..] switch
							{
								"_PUT" => PutOption.instance,
								_ => throw new Exception("Unknown derivative type!"),
							}, price));
						}
					}

				}
				catch (Exception e)
				{
					Console.Error.WriteLine("Exception in derivatives manager thread: " + e.ToString());
				}
				priceCaches.Clear();
				if(waiting.Count > 0){
					try
					{
						Append(waiting.ToArray());
						while (waiting.TryDequeue(out ConcurrentJob result))
						{
							result.Wait();
						}
					}
					catch (Exception e)
					{
						Console.Error.WriteLine("Exception in derivatives manager thread: " + e.ToString());
					}
					waiting.Clear();
				}
				Thread.Sleep(10000);
			}
			Console.WriteLine(Thread.CurrentThread.Name + " stopped!");
		}

		private sealed class DerivativesSettlementJob : ConcurrentJob
		{
			private readonly IDerivativeContract derivativeContract;
			private readonly BigInteger nextStrike;
			private readonly string longname;
			private readonly string shortname;
			private readonly ulong nextExpiry;

			public DerivativesSettlementJob(string longname, string shortname, ulong nextExpiry, IDerivativeContract derivativeContract, BigInteger nextStrike)
			{
				this.longname = longname ?? throw new ArgumentNullException(nameof(longname));
				this.shortname = shortname ?? throw new ArgumentNullException(nameof(shortname));
				this.nextExpiry = nextExpiry;
				this.derivativeContract = derivativeContract ?? throw new ArgumentNullException(nameof(derivativeContract));
				this.nextStrike = nextStrike;
			}

			protected override object ExecuteIMPL()
			{
				//Constants
				using SQLCommandFactory sql = GetSQL(IsolationLevel.RepeatableRead);
				MySqlCommand command = sql.GetCommand("SELECT Strike FROM Derivatives WHERE Name = @coin FOR UPDATE;");
				command.Parameters.AddWithValue("@coin", longname);
				command.Prepare();
				BigInteger strike;
				using(MySqlDataReader tmpreader2 = command.ExecuteReader()){
					CheckSafety(tmpreader2.Read(), "Missing derivatives record!");
					strike = GetBigInteger(tmpreader2.GetString("Strike"));
					tmpreader2.CheckSingletonResult();
				}
				BigInteger longPayouts = derivativeContract.CalculateLongPayout(strike, nextStrike);
				BigInteger shortPayouts = derivativeContract.CalculateShortPayout(strike, nextStrike);
				CheckSafety2(longPayouts.Add(shortPayouts) > derivativeContract.CalculateMaxShortLoss(strike), "Negative balances while settling negative balances protected derivatives!");

				//Balances
				command = sql.GetCommand("SELECT Balance, UserID FROM Balances WHERE Coin = @coin FOR UPDATE;");
				command.Parameters.AddWithValue("@coin", longname);
				command.Prepare();
				Settle2(sql, command, longPayouts, 0);
				command.Parameters["@coin"].Value = shortname;
				Settle2(sql, command, shortPayouts, 1);

				//Outstanding orders
				command = sql.GetCommand("SELECT Amount, InitialAmount, TotalCost, PlacedBy FROM Orders WHERE Sec = @coin AND Buy = 0 FOR UPDATE;");
				command.Parameters.AddWithValue("@coin", longname);
				command.Prepare();
				Settle2(sql, command, longPayouts, 2);

				//Delete old data
				command = sql.GetCommand("DELETE FROM Balances WHERE Coin = @coin;");
				command.Parameters.AddWithValue("@coin", longname);
				command.Prepare();
				command.ExecuteNonQuery();

				command.Parameters["@coin"].Value = shortname;
				command.ExecuteNonQuery();

				command = sql.GetCommand("DELETE FROM Orders WHERE Sec = @coin AND Buy = 0;");
				command.Parameters.AddWithValue("@coin", longname);
				command.Prepare();
				command.ExecuteNonQuery();

				//Update expiry
				command = sql.GetCommand("UPDATE Derivatives SET Expiry = " + nextExpiry + ", Strike = " + nextStrike + " WHERE Name = @coin;");
				command.Parameters.AddWithValue("@coin", longname);
				command.Prepare();
				command.SafeExecuteNonQuery();

				//Flush
				sql.DestroyTransaction(true, false);
				return null;
			}

			private void Settle2(SQLCommandFactory sql, MySqlCommand mySqlCommand, BigInteger payout, byte mode){
				if(!payout.IsZero){
					using MySqlDataReader reader = mySqlCommand.ExecuteReader();
					while (reader.Read())
					{
						BigInteger balance;
						ulong userid;
						if (mode == 2)
						{
							balance = GetBigInteger(reader.GetString("Amount")).Min(GetBigInteger(reader.GetString("InitialAmount")).Sub(GetBigInteger(reader.GetString("TotalCost"))));
							userid = reader.GetUInt64("PlacedBy");
						}
						else
						{
							balance = GetBigInteger(reader.GetString("Balance"));
							userid = reader.GetUInt64("UserID");
						}
						if (userid != 0)
						{
							sql.Credit("Dai", userid, payout.Mul(balance).Div(ether), true);
						}
					}
				}
			}
		}

		//NOTE: JLEX Derivatives have artificially capped profits to protect against unbacked and negative balances for short selling.
		private interface IDerivativeContract
		{
			/// <summary>
			/// The maximum loss short positions can incur, and the maximum profits long positions can make
			/// </summary>
			public abstract BigInteger CalculateMaxShortLoss(BigInteger strike);
			/// <summary>
			/// Calculates the payout for long positions
			/// </summary>
			public abstract BigInteger CalculateLongPayout(BigInteger strike, BigInteger price);
			/// <summary>
			/// Calculates the payout for short positions
			/// </summary>
			public abstract BigInteger CalculateShortPayout(BigInteger strike, BigInteger price);
		}

		/// <summary>
		/// Cash-settled put options pays the holder the diffrence between the current and
		/// strike prices if the underlying asset is trading below the strike price.
		/// </summary>
		private sealed class PutOption : IDerivativeContract
		{
			public static readonly IDerivativeContract instance = new PutOption();
			private PutOption(){
				
			}
			public BigInteger CalculateLongPayout(BigInteger strike, BigInteger price)
			{
				if (price > strike)
				{
					//If the underlying asset is more expensive than the strike price
					//the option expires worthless
					return zero;
				}
				else
				{
					return strike.Sub(price);
				}
			}

			public BigInteger CalculateMaxShortLoss(BigInteger strike)
			{
				return strike;
			}

			public BigInteger CalculateShortPayout(BigInteger strike, BigInteger price)
			{
				return price.Min(strike);
			}
		}
	}
}
