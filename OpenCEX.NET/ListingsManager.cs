using System;
using System.Collections.Generic;
using System.Text;

namespace jessielesbian.OpenCEX {
	public enum CoinType : byte{
		//Depositable and withdrawable
		Native = 0, ERC20 = 1,

		//LP tokens are withdraw only
		LP = 2,

		//Not depositable and withdrawalable (e.g options)
		Pseudo = 3
	}
	public struct CoinDescriptor{
		public readonly string address;
		public readonly BlockchainManager blockchainManager;
		public readonly CoinType coinType;

		//NOTE: Balances caching around options is unsafe
		public readonly bool noncached;
		public readonly bool cached;
		public CoinDescriptor(string address, BlockchainManager blockchainManager, CoinType coinType, bool noncached)
		{
			this.address = address ?? throw new ArgumentNullException(nameof(address));
			this.blockchainManager = blockchainManager ?? throw new ArgumentNullException(nameof(blockchainManager));
			this.noncached = noncached;
			cached = !noncached;
			this.coinType = coinType;
		}
	}
	public struct Pair{
		public readonly string name0;
		public readonly string name1;

		public Pair(string name0, string name1)
		{
			this.name0 = name0 ?? throw new ArgumentNullException(nameof(name0));
			this.name1 = name1 ?? throw new ArgumentNullException(nameof(name1));
		}
	}
	public static partial class StaticUtils{
		private static readonly IDictionary<string, BlockchainManager> blockchains = new Dictionary<string, BlockchainManager>();
		private static readonly IDictionary<ulong, BlockchainManager> blockchains2 = new Dictionary<ulong, BlockchainManager>();
		private static readonly IDictionary<string, CoinDescriptor> coins = new Dictionary<string, CoinDescriptor>();
		private static readonly IDictionary<string, Pair> pairs = new Dictionary<string, Pair>();
		public static void ListChain(string name, string node, ulong chainid){
			BlockchainManager bm = new BlockchainManager(node, chainid);
			CheckSafety(blockchains.TryAdd(name, bm), "Blockchain already listed (should not reach here)!", true);
			try{
				CheckSafety(blockchains2.TryAdd(chainid, bm), "Duplicate chainid (should not reach here)!", true);
			} catch{
				blockchains.Remove(name);
				throw;
			}
			
		}
		public static void ListCoin(string name, string address, string blockchain){
			CheckSafety(blockchains.TryGetValue(blockchain, out BlockchainManager tmp), "Blockchain not found (should not reach here)!", true);
			CheckSafety(coins.TryAdd(name, new CoinDescriptor(address, tmp, (address == string.Empty) ? CoinType.Native : CoinType.ERC20, false)), "Coin already listed (should not reach here)!", true);
		}
		public static void ListPair(string pri, string sec){
			CheckSafety(coins.TryGetValue(pri, out CoinDescriptor tmp), "Base coin not found (should not reach here)!", true);
			CheckSafety2((byte)tmp.coinType == 2, "Base coin must not be a pseudo-token or LP token (should not reach here)!");
			CheckSafety(coins.TryGetValue(pri, out tmp), "Quote coin not found (should not reach here)!", true);
			CheckSafety2((byte)tmp.coinType == 2, "Quote coin must not be an LP token (should not reach here)!");
			StringBuilder namebuilder = new StringBuilder(sec);
			namebuilder.Replace("_", "__");
			namebuilder.Append('_');
			namebuilder.Insert(0, pri);
			CheckSafety(pairs.TryAdd(namebuilder.ToString(), new Pair(pri, sec)), "Pair already listed (should not reach here)!", true);
			if(tmp.coinType != CoinType.Pseudo){
				//NOTE: Uniswap.NET Options Liquidity is unsupported!
				CheckSafety(coins.TryAdd(namebuilder.Insert(0, "LP_").ToString(), new CoinDescriptor("", BlockchainManager.Fake, CoinType.LP, false)), "LP token already listed (should not reach here)!", true);
			}
		}
	}
}