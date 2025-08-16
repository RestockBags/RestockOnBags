const { WebSocket } = require("ws");
const fs = require("fs");
const XLSX = require("xlsx");
const { Connection, PublicKey, Keypair, Transaction, SystemProgram, sendAndConfirmTransaction } = require("@solana/web3.js");
const bs58 = require("bs58");
const config = require("./config.json"); 

const TARGET_MINT_ADDRESS = "FU9etYuLtzANF59y5oNoByLge3raMEUn7EC56LkuBAGS";
const SOL_MINT_ADDRESSES = [
  "So11111111111111111111111111111111111111111", 
  "So11111111111111111111111111111111111111112"  
];
const OUTPUT_FILE = "trades.xlsx";
const HELIUS_RPC_ENDPOINT = config.heliusRpcEndpoint;
const TREASURY_PRIVATE_KEY = config.treasuryPrivateKey;
const ENABLE_REFUNDS = config.enableRefunds !== undefined ? config.enableRefunds : false;
const RATE_LIMIT_PER_SECOND = 9;
const REFUND_CHECK_INTERVAL = 300000;
const MAX_WRITE_RETRIES = 3;

const solanaConnection = new Connection(HELIUS_RPC_ENDPOINT, "confirmed");

let treasuryKeypair;
let treasuryWallet;
try {
  treasuryKeypair = Keypair.fromSecretKey(bs58.decode(TREASURY_PRIVATE_KEY));
  treasuryWallet = treasuryKeypair.publicKey.toString();
} catch (error) {
  console.error("Error initializing treasury wallet:", error.message);
  if (ENABLE_REFUNDS) throw new Error("Treasury private key invalid, required for refunds");
}

const uniqueWallets = new Set();

const requestQueue = [];
let requestCount = 0;
let lastResetTime = Date.now();
const RATE_LIMIT_INTERVAL = 1000;

async function rateLimitedGetOwnerWallet(ataAddress) {
  const now = Date.now();
  if (now - lastResetTime >= RATE_LIMIT_INTERVAL) {
    requestCount = 0;
    lastResetTime = now;
  }

  if (requestCount >= RATE_LIMIT_PER_SECOND) {
    await new Promise((resolve) => setTimeout(resolve, RATE_LIMIT_INTERVAL - (now - lastResetTime)));
    return rateLimitedGetOwnerWallet(ataAddress);
  }

  requestCount++;
  try {
    const ataPublicKey = new PublicKey(ataAddress);
    const accountInfo = await solanaConnection.getAccountInfo(ataPublicKey);
    if (!accountInfo) {
      console.warn(`No account info for ${ataAddress}: Account does not exist`);
      return ataAddress;
    }

    const TOKEN_PROGRAM_IDS = [
      "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
      "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
    ];
    if (TOKEN_PROGRAM_IDS.includes(accountInfo.owner.toString())) {
      const ownerPublicKey = new PublicKey(accountInfo.data.slice(32, 64));
      console.log(`Resolved ATA ${ataAddress} to owner ${ownerPublicKey.toString()}`);
      return ownerPublicKey.toString();
    } else {
      console.warn(`Address ${ataAddress} is not a valid SPL/Token-2022 account (owner: ${accountInfo.owner.toString()})`);
      return ataAddress;
    }
  } catch (error) {
    console.error(`Error resolving owner for ATA ${ataAddress}:`, error.message);
    return ataAddress;
  }
}

let workbook;
if (fs.existsSync(OUTPUT_FILE)) {
  workbook = XLSX.readFile(OUTPUT_FILE);
} else {
  workbook = XLSX.utils.book_new();
  const ws = XLSX.utils.json_to_sheet([], { header: ["ATA", "sol_buy_amount", "status", "solscan_link"] });
  XLSX.utils.book_append_sheet(workbook, ws, "Trades");
  try {
    XLSX.writeFileSync(workbook, OUTPUT_FILE);
    console.log(`Initialized ${OUTPUT_FILE} with headers`);
  } catch (err) {
    console.error(`Error initializing ${OUTPUT_FILE}:`, err.message);
  }
}

function appendTradeToExcel(ata, solAmount) {
  const ws = workbook.Sheets["Trades"];
  const trades = XLSX.utils.sheet_to_json(ws);
  trades.push({ ATA: ata, sol_buy_amount: solAmount, status: 0, solscan_link: "" });
  const newWs = XLSX.utils.json_to_sheet(trades, { header: ["ATA", "sol_buy_amount", "status", "solscan_link"] });
  workbook.Sheets["Trades"] = newWs;
  for (let retries = 0; retries < MAX_WRITE_RETRIES; retries++) {
    try {
      XLSX.writeFileSync(workbook, OUTPUT_FILE);
      console.log(`Appended trade to ${OUTPUT_FILE}: ${ata}, ${solAmount}`);
      return true;
    } catch (err) {
      console.error(`Error writing to ${OUTPUT_FILE} (attempt ${retries + 1}):`, err.message);
      if (retries === MAX_WRITE_RETRIES - 1) {
        console.error(`Failed to write to ${OUTPUT_FILE} after ${MAX_WRITE_RETRIES} attempts`);
        return false;
      }
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1000);
    }
  }
}

function updateExcelRow(index, updates) {
  const ws = workbook.Sheets["Trades"];
  const trades = XLSX.utils.sheet_to_json(ws);
  trades[index] = { ...trades[index], ...updates };
  const newWs = XLSX.utils.json_to_sheet(trades, { header: ["ATA", "sol_buy_amount", "status", "solscan_link"] });
  workbook.Sheets["Trades"] = newWs;
  for (let retries = 0; retries < MAX_WRITE_RETRIES; retries++) {
    try {
      XLSX.writeFileSync(workbook, OUTPUT_FILE);
      console.log(`Updated row ${index + 1} in ${OUTPUT_FILE}: ${JSON.stringify(updates)}`);
      return true;
    } catch (err) {
      console.error(`Error updating ${OUTPUT_FILE} (attempt ${retries + 1}):`, err.message);
      if (retries === MAX_WRITE_RETRIES - 1) {
        console.error(`Failed to update ${OUTPUT_FILE} after ${MAX_WRITE_RETRIES} attempts`);
        return false;
      }
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1000);
    }
  }
}

async function processRefunds() {
  if (!ENABLE_REFUNDS) {
    console.log("Refund processing disabled (ENABLE_REFUNDS=false)");
    return;
  }

  console.log("Checking for pending refunds...");
  const ws = workbook.Sheets["Trades"];
  const trades = XLSX.utils.sheet_to_json(ws);
  const treasuryBalanceLamports = await solanaConnection.getBalance(new PublicKey(treasuryWallet));
  const treasuryBalance = treasuryBalanceLamports / 1e9;
  console.log(`Treasury balance: ${treasuryBalance} SOL`);

  for (let i = 0; i < trades.length; i++) {
    if (trades[i].status !== 0) continue;

    const ata = trades[i].ATA;
    const solBuyAmount = parseFloat(trades[i].sol_buy_amount);
    const requiredBalance = 2 + 2 * solBuyAmount;

    if (treasuryBalance >= requiredBalance) {
      const ownerWallet = await rateLimitedGetOwnerWallet(ata);
      if (ownerWallet === ata) {
        console.warn(`Could not resolve owner for ATA ${ata}, skipping refund for row ${i + 1}`);
        return;
      }

      try {
        const transaction = new Transaction().add(
          SystemProgram.transfer({
            fromPubkey: treasuryKeypair.publicKey,
            toPubkey: new PublicKey(ownerWallet),
            lamports: Math.floor(solBuyAmount * 1e9)
          })
        );
        const signature = await sendAndConfirmTransaction(solanaConnection, transaction, [treasuryKeypair]);
        const solscanLink = `https://solscan.io/tx/${signature}`;
        console.log(`Refunded ${solBuyAmount} SOL to ${ownerWallet} (tx: ${signature}, link: ${solscanLink})`);

        const updated = updateExcelRow(i, { status: 1, solscan_link: solscanLink });
        if (!updated) {
          console.error(`Failed to update status for ${ata}, refund completed but status not updated`);
          return;
        }

        return;
      } catch (error) {
        console.error(`Error sending refund to ${ownerWallet} for ATA ${ata}:`, error.message);
        return;
      }
    } else {
      console.log(`Insufficient treasury balance (${treasuryBalance} SOL) for refund of ${solBuyAmount} SOL to ATA ${ata} (requires ${requiredBalance} SOL)`);
      return;
    }
  }
  console.log("No more pending refunds to process.");
}

if (ENABLE_REFUNDS) {
  setInterval(processRefunds, REFUND_CHECK_INTERVAL);
  processRefunds();
} else {
  console.log("Refund processing disabled on startup (ENABLE_REFUNDS=false)");
}

const bitqueryConnection = new WebSocket(
  "wss://streaming.bitquery.io/eap?token=" + config.oauthtoken,
  ["graphql-ws"]
);

bitqueryConnection.on("open", () => {
  console.log("Connected to Bitquery.");

  const initMessage = JSON.stringify({ type: "connection_init" });
  bitqueryConnection.send(initMessage);
});

bitqueryConnection.on("message", (data) => {
  const response = JSON.parse(data.toString());

  switch (response.type) {
    case "connection_ack":
      console.log("Connection acknowledged by server.");

      const meteoraSubscription = JSON.stringify({
        type: "start",
        id: "1",
        payload: {
          query: `
            subscription {
              Solana {
                DEXTrades(
                  where: {Trade: {Dex: {ProgramAddress: {is: "dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"}}}}
                ) {
                  Trade {
                    Dex {
                      ProgramAddress
                      ProtocolFamily
                      ProtocolName
                    }
                    Buy {
                      Currency {
                        Name
                        Symbol
                        MintAddress
                      }
                      Amount
                      Account {
                        Address
                      }
                      PriceAgainstSellCurrency: Price
                    }
                    Sell {
                      Account {
                        Address
                      }
                      Amount
                      Currency {
                        Name
                        Symbol
                        MintAddress
                      }
                      PriceAgainstBuyCurrency: Price
                    }
                  }
                  Block {
                    Time
                  }
                }
              }
            }
          `,
        },
      });

      const postMigrationSubscription = JSON.stringify({
        type: "start",
        id: "2",
        payload: {
          query: `
            subscription {
              Solana {
                DEXTrades(
                  where: {
                    Trade: {
                      Dex: {
                        ProgramAddress: {
                          is: "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"
                        }
                      }
                    }
                  }
                ) {
                  Trade {
                    Dex {
                      ProgramAddress
                      ProtocolFamily
                      ProtocolName
                    }
                    Buy {
                      Currency {
                        Name
                        Symbol
                        MintAddress
                      }
                      Amount
                      Account {
                        Address
                      }
                      PriceAgainstSellCurrency: Price
                    }
                    Sell {
                      Account {
                        Address
                      }
                      Amount
                      Currency {
                        Name
                        Symbol
                        MintAddress
                      }
                      PriceAgainstBuyCurrency: Price
                    }
                  }
                  Block {
                    Time
                  }
                }
              }
            }
          `,
        },
      });

      bitqueryConnection.send(meteoraSubscription);
      console.log("Subscription message sent for Meteora DBC trades (ID: 1).");
      bitqueryConnection.send(postMigrationSubscription);
      console.log("Subscription message sent for post-migration bonded token trades (ID: 2).");
      break;

    case "data":
      const subscriptionId = response.id;
      const querySource = subscriptionId === "1" ? "Meteora DBC" : "Post-Migration";
      console.log(`Raw data response (${querySource}):`, JSON.stringify(response.payload.data, null, 2));
      const trades = response.payload.data?.Solana?.DEXTrades || [];
      console.log(`Received ${trades.length} trades from ${querySource}`);

      trades.forEach((trade, index) => {
        const buy = trade.Trade.Buy;
        const sell = trade.Trade.Sell;
        const buyerAddress = buy.Account.Address;
        const sellCurrency = sell.Currency.MintAddress;
        const solAmount = SOL_MINT_ADDRESSES.includes(sellCurrency) ? sell.Amount : 0;
        const buyMint = buy.Currency.MintAddress;

        console.log(`Trade ${index + 1} (${querySource}): Buyer=${buyerAddress}, BuyMint=${buyMint}, SellMint=${sellCurrency}, SOL=${solAmount}, Time=${trade.Block.Time}`);

        if (!TARGET_MINT_ADDRESS || buyMint === TARGET_MINT_ADDRESS) {
          if (!uniqueWallets.has(buyerAddress) && solAmount > 0) {
            uniqueWallets.add(buyerAddress);
            if (appendTradeToExcel(buyerAddress, solAmount)) {
              console.log(`Logged trade (${querySource}): ${buyerAddress}, ${solAmount} SOL`);
            }
          } else if (uniqueWallets.has(buyerAddress)) {
            console.log(`Skipped trade ${index + 1} (${querySource}): ${buyerAddress} already logged`);
          } else if (solAmount === 0) {
            console.log(`Skipped trade ${index + 1} (${querySource}): No SOL paid (SellMint=${sellCurrency})`);
          }
        } else {
          console.log(`Skipped trade ${index + 1} (${querySource}): BuyMint=${buyMint} does not match TARGET_MINT_ADDRESS=${TARGET_MINT_ADDRESS}`);
        }
      });
      break;

    case "ka":
      console.log("Keep-alive message received.");
      break;

    case "error":
      console.error("Error message received:", response.payload.errors);
      break;

    default:
      console.warn("Unhandled message type:", response.type);
  }
});

bitqueryConnection.on("close", (code, reason) => {
  console.log(`Disconnected from Bitquery. Code: ${code}, Reason: ${reason}`);
});

bitqueryConnection.on("error", (error) => {
  console.error("WebSocket Error:", error.message);
});