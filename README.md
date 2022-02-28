# expiry-sim-data-generator

Code to convert data from "NF BNF options data (1min) backup" telegram group for generating dataset in a format that's suited for Expiry Replay Simulator that we're building.

Special thanks to the person running the group for providing access to those datasets.

## Usage
1. Please join the group and manually download the files that the admin shares after each expiry and put them all in a folder.
2. In the code we use `experiments/weekly` and `experiments/monthly` folders, but feel free to use anything of your choice.
3. If you do use a different path, you might want to update the path references in `main.go` file.
4. Then run `go run .` from the root of the repo. I'm assuming you've Go installed and have run `go mod download` already.
5. You should be able to find the required data under `data/` directory of the current folder. 

## Data Format
We generate 1 minute, 3 minute, 5 minute and 15 minute OHLC data from the source (1 min) dataset. This ensures we can instanly show / replay the data as needed on the UI as per the user's requirement.

The directory structure of the output is as follows:
```
+-data/
  |
  +-2021-12-23/ // date of the expiry
  | |
  | +-symbols.json // contains the list of symbol under this expiry
  | |
  | +-NIFTYWK15100PE_1min.json // contains the entire expiry data in 1 minute OHLC,OI,VOL format
  | |
  | +-NIFTYWK15100PE_3min.json // contains the entire expiry data in 3 minute OHLC,OI,VOL format
  | |
  | +-NIFTYWK15100PE_5min.json // contains the entire expiry data in 5 minute OHLC,OI,VOL format
  | |
  | +-... <additional files>
  |
  +-expiries.json // contains the list of expiries for each of the underlying (NIFTY, BANKNIFTY)
  |
  +-... <additional files>
```

## License
https://www.apache.org/licenses/LICENSE-2.0
