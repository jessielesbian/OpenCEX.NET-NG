using jessielesbian.OpenCEX.RequestManager;

using System.Runtime.CompilerServices;
using System.Numerics;

namespace jessielesbian.OpenCEX{
	public static class BalanceManager{

		/// <summary>
		/// Credit funds to a customer account.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void Credit(this Request request, string coin, ulong userid, BigInteger amount, bool safe)
		{
			request.sqlCommandFactory.Credit(coin, userid, amount, safe);
		}

		/// <summary>
		/// Debit funds from a customer account.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void Debit(this Request request, string coin, ulong userid, BigInteger amount, bool safe)
		{
			request.sqlCommandFactory.Debit(coin, userid, amount, safe);
		}
	}
}