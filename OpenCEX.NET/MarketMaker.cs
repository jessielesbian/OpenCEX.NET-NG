
using MySql.Data.MySqlClient;
using System;
using System.Text;
using System.Numerics;

//Uniswap.NET: C# port of the Uniswap Automated Market Maker
//OpenCEX uses Uniswap.NET to optimize capital efficency, and matching engine performance.

namespace jessielesbian.OpenCEX{
	
	public static partial class StaticUtils{
		public struct LPReserve
		{
			public readonly BigInteger reserve0;
			public readonly BigInteger reserve1;
			public readonly BigInteger totalSupply;
			public readonly bool insert;
			public LPReserve(SQLCommandFactory sql, string pri, string sec)
			{
				insert = ReadLP(sql, pri, sec, out reserve0, out reserve1, out totalSupply);
			}

			public LPReserve(BigInteger reserve0, BigInteger reserve1, BigInteger totalSupply, bool insert)
			{
				this.reserve0 = reserve0;
				this.reserve1 = reserve1;
				this.totalSupply = totalSupply;
				this.insert = insert;
			}

			public BigInteger QuoteLP(BigInteger inp, bool atob){
				CheckSafety2(inp.IsZero || reserve0.IsZero || reserve1.IsZero, "Uniswap.NET: Zero liquidity!");
				if(atob){
					return inp.Mul(reserve0).Div(reserve1);
				} else{
					return inp.Mul(reserve1).Div(reserve0);
				}
			}
		}
		public static BigInteger Sqrt(this BigInteger y){
			if (y > three)
			{
				BigInteger z = y;
				BigInteger x = y.Div(two).Add(one);
				while (x < z)
				{
					z = x;
					x = y.Div(x).Add(x).Div(two);
				}
				return z;
			}
			else if (y.IsZero)
			{
				return zero;
			} else{
				return one;
			}
		}
		private static bool ReadLP(SQLCommandFactory sql, string primary, string secondary, out BigInteger reserve0, out BigInteger reserve1, out BigInteger totalSupply)
		{
			MySqlCommand mySqlCommand = sql.GetCommand("SELECT Reserve0, Reserve1, TotalSupply FROM UniswapReserves WHERE Pri = @pri AND Sec = @sec FOR UPDATE;");
			mySqlCommand.Parameters.AddWithValue("@pri", primary);
			mySqlCommand.Parameters.AddWithValue("@sec", secondary);
			using MySqlDataReader mySqlDataReader = mySqlCommand.ExecuteReader();
			if (mySqlDataReader.Read())
			{
				reserve0 = GetBigInteger(mySqlDataReader.GetString("Reserve0"));
				reserve1 = GetBigInteger(mySqlDataReader.GetString("Reserve1"));
				totalSupply = GetBigInteger(mySqlDataReader.GetString("TotalSupply"));
				CheckSafety2(mySqlDataReader.Read(), "Uniswap.NET: Duplicate LP records (should not reach here)!", true);
				return false;
			}
			else
			{
				reserve0 = zero;
				reserve1 = zero;
				totalSupply = zero;
				return true;
			}


		}

		private static void WriteLP(SQLCommandFactory sql, string primary, string secondary, LPReserve lpreserve)
		{
			StringBuilder stringBuilder;
			if(lpreserve.insert){
				stringBuilder = new StringBuilder("INSERT INTO UniswapReserves (Reserve0, Reserve1, TotalSupply, Pri, Sec) VALUES (\"");
				stringBuilder.Append(lpreserve.reserve0.ToString() + "\", \"");
				stringBuilder.Append(lpreserve.reserve1.ToString() + "\", \"");
				stringBuilder.Append(lpreserve.totalSupply.ToString() + "\", @pri, @sec);");
			} else{
				stringBuilder = new StringBuilder("UPDATE UniswapReserves SET Reserve0 = \"");
				stringBuilder.Append(lpreserve.reserve0.ToString());
				stringBuilder.Append("\", Reserve1 = \"");
				stringBuilder.Append(lpreserve.reserve1.ToString());
				stringBuilder.Append("\", TotalSupply = \"");
				stringBuilder.Append(lpreserve.totalSupply.ToString());
				stringBuilder.Append("\" WHERE Pri = @pri AND Sec = @sec;");
			}
			MySqlCommand mySqlCommand = sql.GetCommand(stringBuilder.ToString());
			mySqlCommand.Parameters.AddWithValue("@pri", primary);
			mySqlCommand.Parameters.AddWithValue("@sec", secondary);
			mySqlCommand.Prepare();
			mySqlCommand.SafeExecuteNonQuery();
		}

		/// <summary>
		/// Mints Uniswap.NET LP tokens to trader
		/// </summary>
		public static LPReserve MintLP(this SQLCommandFactory sql, string pri, string sec, BigInteger amount0, BigInteger amount1, ulong to, LPReserve lpreserve)
		{
			try
			{
				GetEnv("IsDerivative_" + sec);
				throw new SafetyException("Uniswap.NET derivatives liquidity not yet supported!");
			}
			catch (Exception e)
			{
				if (e is SafetyException)
				{
					throw;
				}
			}
			sql.Debit(pri, to, amount0, true);
			sql.Debit(sec, to, amount1, true);

			
			string name = "LP_" + pri.Replace("_", "__") + "_" + sec;

			BigInteger liquidity;
			if (lpreserve.totalSupply.IsZero)
			{
				liquidity = amount0.Mul(amount1).Sqrt().Sub(thousand, "Uniswap.NET: Insufficent liquidity minted!", false);
			}
			else
			{
				liquidity = amount0.Mul(lpreserve.totalSupply).Div(lpreserve.reserve0).Min(amount1.Mul(lpreserve.totalSupply).Div(lpreserve.reserve1));
			}
			CheckSafety2(liquidity.IsZero, "Uniswap.NET: Insufficent liquidity minted!");
			lpreserve = new LPReserve(lpreserve.reserve0.Add(amount0), lpreserve.reserve1.Add(amount1), lpreserve.totalSupply.Add(liquidity), lpreserve.insert);
			WriteLP(sql, pri, sec, lpreserve);

			sql.Credit(name, to, liquidity, false);
			return lpreserve;
		}

		/// <summary>
		/// Burns Uniswap.NET LP tokens from trader
		/// </summary>
		public static LPReserve BurnLP(this SQLCommandFactory sql, string pri, string sec, BigInteger amount, ulong to, LPReserve lpreserve)
		{
			CheckSafety2(lpreserve.insert, "Uniswap.NET: Burn from empty pool!");
			string name = "LP_" + pri.Replace("_", "__") + "_" + sec;
			sql.Debit(name, to, amount, false);
			BigInteger remainingTotalSupply = lpreserve.totalSupply.Sub(amount, "Uniswap.NET: Burn exceeds total supply (should not reach here)!", true);

			BigInteger out0 = lpreserve.reserve0.Mul(amount).Div(lpreserve.totalSupply);
			BigInteger out1 = lpreserve.reserve1.Mul(amount).Div(lpreserve.totalSupply);
			CheckSafety2(out0.IsZero || out1.IsZero, "Uniswap.NET: Insufficent liquidity burned!");
			lpreserve = new LPReserve(lpreserve.reserve0.Sub(out0), lpreserve.reserve1.Sub(out1), remainingTotalSupply, false);
			WriteLP(sql, pri, sec, lpreserve);

			sql.Credit(pri, to, out0, true);
			sql.Credit(sec, to, out1, true);
			return lpreserve;
		}

		/// <summary>
		/// Burns Uniswap.NET LP tokens from trader
		/// </summary>
		public static LPReserve BurnLP(this SQLCommandFactory sql, string pri, string sec, BigInteger amount, ulong to)
		{
			return sql.BurnLP(pri, sec, amount, to, new LPReserve(sql, pri, sec));
		}

		
		/// <summary>
		/// Swaps tokens using Uniswap.NET (NO MUTATE)
		/// </summary>
		public static LPReserve SwapLP(this SQLCommandFactory sql, string pri, string sec, ulong userid, BigInteger input, bool buy, LPReserve lpreserve, out BigInteger output){
			CheckSafety2(input.IsZero, "Uniswap.NET: Insufficent input amount!");
			CheckSafety2(lpreserve.insert, "Uniswap.NET: Pool does not exist!");
			BigInteger reserveIn;
			BigInteger reserveOut;
			string out_token;
			if(buy){
				out_token = sec;
				reserveIn = lpreserve.reserve0;
				reserveOut = lpreserve.reserve1;
			} else{
				out_token = pri;
				reserveIn = lpreserve.reserve1;
				reserveOut = lpreserve.reserve0;
			}
			CheckSafety2(reserveIn.IsZero || reserveOut.IsZero, "Uniswap.NET: Insufficent liquidity!");

			BigInteger amountInWithFee = input.Mul(afterfees);
			BigInteger numerator = amountInWithFee.Mul(reserveOut);
			BigInteger denominator = reserveIn.Mul(thousand).Add(amountInWithFee);
			output = numerator.Div(denominator);
			CheckSafety2(output.IsZero, "Uniswap.NET: Insufficent output amount!");
			sql.Credit(out_token, userid, output, true);

			if (buy)
			{
				lpreserve = new LPReserve(lpreserve.reserve0.Add(input), lpreserve.reserve1.Sub(output), lpreserve.totalSupply, false);
			}
			else
			{
				lpreserve = new LPReserve(lpreserve.reserve0.Sub(output), lpreserve.reserve1.Add(input), lpreserve.totalSupply, false);
			}
			return lpreserve;
		}

		/// <summary>
		/// Fills order using Uniswap.NET (NO MUTATE)
		/// </summary>
		private static LPReserve TryArb(this SQLCommandFactory sqlCommandFactory, string primary, string secondary, bool buy, Order instance, BigInteger price, LPReserve lpreserve)
		{
			try
			{
				//Optimization: If it is a derivative then DON'T EVEN BOTHER 
				GetEnv("IsDerivative_" + secondary);
				return lpreserve;
			} catch{
				
			}
			if (lpreserve.reserve0.IsZero || lpreserve.reserve1.IsZero){
				return lpreserve;
			}
			BigInteger ArbitrageIn;
			BigInteger invariant = lpreserve.reserve0.Mul(lpreserve.reserve1);
			if (invariant.IsZero)
			{
				return lpreserve;
			}
			else
			{
				if (buy == lpreserve.reserve0.Mul(price).Div(lpreserve.reserve1) < ether){
					BigInteger leftSide;
					if(buy){
						leftSide = invariant.Mul(thousand).Mul(ether).Div(price.Mul(afterfees)).Sqrt();
					} else{
						leftSide = invariant.Mul(thousand).Mul(price).Div(afterether).Sqrt();
					}

					BigInteger rightSide = ether.Mul(thousand).Div(afterfees);
					if (leftSide < rightSide)
					{
						return lpreserve;
					}
					else
					{
						ArbitrageIn = leftSide.Sub(rightSide);
					}
				} else{
					return lpreserve;
				}
			}


			ArbitrageIn = ArbitrageIn.Min(instance.Balance);


			if (ArbitrageIn.IsZero)
			{
				return lpreserve;
			} else{
				//Partial order cancellation
				if(buy){
					instance.Debit(ArbitrageIn.Mul(ether).Div(price), price);
				} else{
					instance.Debit(ArbitrageIn, ether);
				}

				//Swap using Uniswap.NET
				return sqlCommandFactory.SwapLP(primary, secondary, instance.placedby, ArbitrageIn, buy, lpreserve, out _);
			}
		}
	}
}