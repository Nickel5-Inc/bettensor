<div align="center">

![Bettensor Logo](./docs/assets/bettensor-twitter-header.jpg) 





A sports prediction subnet on the Bittensor network

[Installation](#installation-and-setup) • [Validators](#guide-for-validators) • [Miners](#guide-for-miners) • [Release Details](#details-for-current-release-version-v001-beta) • [Website](https://bettensor.com) • [Official Discord Server](https://discord.gg/YVyVHHEd) 

</div>

## What Is Bettensor?

Bettensor is a sports prediction subnet. The goal of Bettensor is to provide a platform for sports fans to predict the outcomes of their favorite sporting events, and ML/AI researchers to develop new models and strategies to benchmark against good, old fashioned human intelligence and intuition. 

Mining on this subnet is simple. In this Beta release, miners receive upcoming games and odds, submit predictions as a simulated wager, and are rewarded for correct predictions upon game conclusion. Compute requirements are minimal for those choosing the human intelligence method - all you need is to consistently submit predictions for upcoming games.



> [!IMPORTANT]
> **Before you begin using our services, please read this legal disclaimer carefully:**

>- **Nature of Service:** Bettensor is a software application developed on the Bittensor network, which operates independently of our control. We are not a financial institution, nor do we facilitate any monetary transactions directly. Bettensor does not distribute payments nor collects money, whether in the form of cryptocurrencies or fiat currencies.

>- **Rewards and Transactions:** Users of Bettensor engage directly with the Bittensor network. Rewards or payments are issued by the Bittensor network based on the protocols established within its framework. Bettensor's role is limited to providing software that suggests allocation of $TAO tokens based on the predictions made by users. We do not influence or control the decision-making process of the Bittensor network concerning the distribution of rewards.

>- **No Financial Advice:** The information provided by Bettensor is for informational purposes only and should not be considered financial advice. Users should conduct their own research or consult with a professional advisor before engaging in any betting activities.

>- **Assumption of Risk:** By using Bettensor, users acknowledge and accept the risks associated with online betting and digital transactions, including but not limited to the risk of financial loss. Users should engage with the platform responsibly and within the bounds of applicable laws and regulations.

>- **Compliance with Laws:** Users of Bettensor are solely responsible for ensuring that their actions comply with local, state, and federal laws applicable to sports betting and online gambling. Bettensor assumes no responsibility for illegal or unauthorized use of the service.

>- **Changes and Amendments:** We reserve the right to modify this disclaimer at any time. Users are encouraged to periodically review this document to stay informed of any changes.

>By using Bettensor, you acknowledge that you have read, understood, and agreed to the terms outlined in this legal disclaimer. If you do not agree with any part of this disclaimer, you should not use Bettensor.


## Installation and Setup

To mine or validate on this subnet, we recommend starting with a cheap VPS instance running Ubuntu 22.04. As with most Subnets, we also recommend running your own Lite Node. You can find a guide to running a Lite Node [here](https://docs.bittensor.com/subtensor-nodes/). 

>[!NOTE]
>In this current Beta version, we require Bittensor v6.9.3.

1. Clone the repository:
```bash
git clone https://github.com/bettensor/bettensor.git
```

2. Update apt-get:
```bash
sudo apt-get update
```

4. Run the setup script:
```bash
cd bettensor
chmod +x scripts/setup.sh
source ./scripts/setup.sh
```
   - if you want to set up a lite node (recommended), run the command with the flag `source ./scripts/setup.sh --lite-node`

   - additionally, the script takes `--subtensor.network` as an optional flag. if you want to run the lite node on testnet, run `source ./scripts/setup.sh --subtensor.network test` , or `main` for mainnet.

7. Set up a Bittensor wallet (guide [here](https://docs.bittensor.com/getting-started/wallets)).

8. Register on the subnet:

- Mainnet `(NETUID 30)`:

 ```bash
btcli subnet register --netuid 30 --wallet.name <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY>
 ```
- Testnet `(NETUID: 181)`:

 ```bash
btcli subnet register --netuid 181 --wallet.name <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY> --subtensor.network test
 ```




## Guide for Validators

For detailed instructions on setting up and running a Bettensor validator, please refer to our [Validator Guide](docs/validating.md). This document covers:

- Setting up the environment
- Running the validator
- Recommended logging settings

Whether you're new to Bettensor or an experienced validator, the Validator Guide provides all the information you need to participate in the network effectively.




## Guide for Miners

For detailed instructions on setting up and running a Bettensor miner, please refer to our [Miner Guide](docs/mining.md). This comprehensive document covers:

- Miner setup and configuration
- Choosing between Local and Central Server interfaces
- Submitting predictions
- Managing multiple miners
- Security considerations
- Troubleshooting and FAQs

Whether you're new to Bettensor or an experienced miner, the Miner Guide provides all the information you need to participate in the network effectively.




## Incentive Mechanism and Scoring
The incentive mechanism is intricate and rewards the best miners disproportionately. Details around the mechanism can be found [here](https://nickel5.substack.com/p/sports-prediction-and-betting-models).


## Minimum Staking Requirement

To ensure commitment to the network and prevent abuse, miners are required to meet a minimum staking threshold to receive weight from validators. This stake requirement helps maintain network quality and security.

### How to Add Stake to Your Hotkey

Miners can earn stake through incentive mechanisms or by manually adding it using the btcli command:

#### Adding stake to a single hotkey:
```bash
btcli stake add --amount .3 --netuid 30 --wallet.name <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY>
```

#### Adding stake to multiple hotkeys simultaneously:
```bash
btcli stake add --amount .6 --netuid 30 --wallet.name <YOUR_COLDKEY> --wallet.hotkey hotkey1,hotkey2
```

>[!NOTE]
>The minimum stake amount is decided by validators. Miners without sufficient stake will not receive weight in the network, regardless of their performance or tier. Make sure to check your stake levels regularly.


## Details for Current Release Version (v2.0.0, Beta)

- Currently Supported Sports: Baseball, Football (Soccer), American Football
- Currently available base models: (Football (Soccer): UEFA Teams, NFL)
- Requires Bittensor==v6.9.4. Support for Bittensor v7.x is coming soon.





## License

This repository is licensed under the MIT License.

```text
The MIT License (MIT)
Copyright © 2024 Bettensor (oneandahalfcats, geardici, honeybadgerhavoc)

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
```
<div align="center">

![Bettensor Logo](./docs/assets/bettensor_spin_animation.gif) 
</div>