
using System.Numerics;
using System.Collections;
using System.Linq;
using System;
using System.Globalization;
using Nethereum.Hex.HexTypes;
using System.Runtime.CompilerServices;

namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils{
		public static readonly BigInteger day = new BigInteger(86400);
		public static readonly BigInteger basegas = new BigInteger(21000);
		public static readonly BigInteger basegas2 = new BigInteger(1000000);
		public static readonly BigInteger e16 = new BigInteger(65536);
		public static readonly BigInteger ten = new BigInteger(10);
		public static readonly BigInteger ether = GetBigInteger("1000000000000000000");
		public static readonly BigInteger zero = BigInteger.Zero;
		public static readonly BigInteger one = BigInteger.One;
		public static readonly BigInteger two = new BigInteger(2);
		public static readonly BigInteger three = new BigInteger(3);
		public static readonly BigInteger thousand = new BigInteger(1000);
		public static readonly BigInteger afterfees = new BigInteger(997);
		public static readonly BigInteger afterether = GetBigInteger("997000000000000000000");

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger GetBigInteger(string number){
			if (number.StartsWith("0x"))
			{
				number = number[2..];
				if (number[0] != '0'){
					number = '0' + number;
				}
				if(number.Length % 2 == 1){
					number = '0' + number;
				}
				return BigInteger.Parse(number, NumberStyles.AllowHexSpecifier);
			}
			else{
				return BigInteger.Parse(number, NumberStyles.None);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static string SafeSerializeBigInteger(BigInteger stuff){
			return stuff.ToString();
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger ExtractBigInteger(this RequestManager.Request request, string key)
		{
			string temp = request.ExtractRequestArg<string>(key);
			string postfix = key + '!';
			CheckSafety2(temp.Length == 0, "Zero-length number for request argument: " + postfix);
			CheckSafety2(temp[0] == '-', "Negative number for request argument: " + postfix);
			try{
				return GetBigInteger(temp);
			} catch{
				throw new SafetyException("Invalid number for request argument: " + postfix);
			}
		}

		//Extend big-integs
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static string ToHex(this BigInteger bn,bool prefix = true, bool pad256 = true)
		{
			CheckSafety2(bn < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			string postfix = new HexBigInteger(bn).HexValue.ToLower()[2..];
			if (pad256)
			{
				CheckSafety2(postfix.Length > 64, "256-bit integer overflow!");
				postfix = postfix.PadLeft(64, '0');
			}
			else
			{
				while (postfix.StartsWith('0'))
				{
					postfix = postfix[1..];
				}
				if (postfix == string.Empty)
				{
					postfix = "0";
				}
			}

			if (prefix)
			{
				return "0x" + postfix;
			}
			else
			{
				return postfix;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger Sub(this BigInteger bigInteger, BigInteger other)
		{
			CheckSafety2(bigInteger < 0 && other < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			other = bigInteger - other;
			CheckSafety2(bigInteger < BigInteger.Zero, "SafeMath: Subtraction Overflow (should not reach here)!", true);
			return other;
		}
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger Sub(this BigInteger bigInteger, BigInteger other, string msg, bool critical)
		{
			CheckSafety2(bigInteger < 0 && other < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			other = bigInteger - other;
			CheckSafety2(bigInteger < BigInteger.Zero, msg, critical);
			return other;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger Div(this BigInteger bigInteger, BigInteger other)
		{
			CheckSafety2(bigInteger < 0 && other < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			CheckSafety2(other.IsZero, "SafeMath: Unexpected division by zero (should not reach here)!", true);
			return bigInteger / other;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger Mod(this BigInteger bigInteger, BigInteger other)
		{
			CheckSafety2(bigInteger < 0 && other < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			CheckSafety2(other.IsZero, "SafeMath: Unexpected modulo by zero (should not reach here)!", true);
			return bigInteger / other;
		}
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger Mul(this BigInteger bigInteger, BigInteger other)
		{
			CheckSafety2(bigInteger < 0 && other < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			return bigInteger * other;
		}
		public static BigInteger Add(this BigInteger bigInteger, BigInteger other)
		{
			CheckSafety2(bigInteger < 0 && other < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			return bigInteger + other;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger Max(this BigInteger bigInteger, BigInteger other)
		{
			CheckSafety2(bigInteger < 0 && other < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			if (bigInteger > other)
			{
				return bigInteger;
			}
			else
			{
				return other;
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static BigInteger Min(this BigInteger bigInteger, BigInteger other)
		{
			CheckSafety2(bigInteger < 0 && other < 0, "SafeMath: Unexpected negative numbers (should not reach here)!", true);
			if (bigInteger > other)
			{
				return other;
			}
			else
			{
				return bigInteger;
			}
		}
	}
}