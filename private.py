"""
private.py — Standalone Seed Phrase → Private Key Converter
============================================================
Converts a BIP39 mnemonic (seed phrase) to the Ethereum/Polygon private key
used by Polymarket.

Standard derivation path: m/44'/60'/0'/0/0  (same for ETH and Polygon)

Usage:
    python private.py
    python private.py "word1 word2 ... word12"

Requirements (already in requirements.txt):
    pip install eth-account

Optional (for extra BIP32 support):
    pip install mnemonic bip32utils
"""

import sys


def derive_from_eth_account(mnemonic: str, account_index: int = 0) -> dict:
    """
    Derive private key using eth-account (already installed in this project).
    Supports 12 and 24 word mnemonics.
    """
    from eth_account import Account

    # eth-account requires HD wallet features to be enabled
    Account.enable_unaudited_hdwallet_features()

    path = f"m/44'/60'/0'/0/{account_index}"
    acct = Account.from_mnemonic(mnemonic.strip(), account_path=path)

    return {
        "address":     acct.address,
        "private_key": acct.key.hex(),
        "path":        path,
    }


def derive_multiple_accounts(mnemonic: str, count: int = 5) -> list:
    """Derive the first `count` accounts from the mnemonic."""
    return [derive_from_eth_account(mnemonic, i) for i in range(count)]


def validate_mnemonic(mnemonic: str) -> bool:
    """Basic validation: BIP39 mnemonics are 12, 15, 18, 21, or 24 words."""
    words = mnemonic.strip().split()
    return len(words) in (12, 15, 18, 21, 24)


def main():
    print("=" * 60)
    print("  Polymarket Private Key Converter")
    print("  Seed Phrase → Ethereum/Polygon Private Key")
    print("=" * 60)
    print()

    # Accept mnemonic from command-line arg or prompt
    if len(sys.argv) > 1:
        mnemonic = " ".join(sys.argv[1:])
        print(f"Using mnemonic from command-line argument.")
    else:
        print("Enter your seed phrase (12 or 24 words, space-separated):")
        print("(input is NOT shown for security)")
        import getpass
        mnemonic = getpass.getpass("> ").strip()

    print()

    if not mnemonic:
        print("ERROR: No mnemonic provided.")
        sys.exit(1)

    if not validate_mnemonic(mnemonic):
        word_count = len(mnemonic.split())
        print(f"WARNING: Mnemonic has {word_count} words. Expected 12, 15, 18, 21, or 24.")
        print("Proceeding anyway...")
        print()

    try:
        # Derive default account (index 0)
        result = derive_from_eth_account(mnemonic, account_index=0)

        print("─" * 60)
        print("  ACCOUNT 0  (default, used by most wallets)")
        print("─" * 60)
        print(f"  Wallet Address : {result['address']}")
        print(f"  Private Key    : {result['private_key']}")
        print(f"  Derivation Path: {result['path']}")
        print()

        show_more = input("Show accounts 1-4 as well? (y/N): ").strip().lower()
        if show_more == "y":
            print()
            for i in range(1, 5):
                r = derive_from_eth_account(mnemonic, account_index=i)
                print(f"  ACCOUNT {i}")
                print(f"  Address    : {r['address']}")
                print(f"  Private Key: {r['private_key']}")
                print(f"  Path       : {r['path']}")
                print()

        print("─" * 60)
        print("  How to use in Polymarket Bot:")
        print("─" * 60)
        print()
        print("  Option A — settings.json (persists across restarts):")
        print(f'  Set "private_key": "{result["private_key"]}"')
        print(f'  inside the "polymarket" section of settings.json')
        print()
        print("  Option B — .env file:")
        print(f"  POLYMARKET__PRIVATE_KEY={result['private_key']}")
        print()
        print("  Option C — paste into the Settings tab > Private Key field")
        print("  in the dashboard (stores in memory only).")
        print()
        print("  NOTE: The bot uses account 0 by default. Make sure")
        print("  this matches the wallet you funded on Polymarket.")
        print()

    except ImportError:
        print("ERROR: eth-account is not installed.")
        print("Run:  pip install eth-account")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        print()
        print("Common causes:")
        print("  - Invalid mnemonic (wrong words or wrong number of words)")
        print("  - Mnemonic has a typo")
        sys.exit(1)


if __name__ == "__main__":
    main()
