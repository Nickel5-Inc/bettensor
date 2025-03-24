# Bettensor Miner Guide

This guide provides detailed information for setting up and running a Bettensor miner. Whether you're new to the network or an experienced user, this document will help you get started and make the most of your mining experience.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Miner Setup](#miner-setup)
3. [Choosing Your Interface](#choosing-your-interface)
4. [Running Your Miner](#running-your-miner)
5. [Minimum Stake Requirement](#minimum-stake-requirement)
6. [Submitting Predictions](#submitting-predictions)
7. [Model Predictions](#model-predictions)
8. [Managing Multiple Miners](#managing-multiple-miners)
9. [Security Considerations](#security-considerations)
10. [Troubleshooting](#troubleshooting)
11. [Frequently Asked Questions](#frequently-asked-questions)
12. [Database Setup](#database-setup)

## Getting Started

Before you begin, ensure you have the following prerequisites:

- Python 3.8 or higher installed
- Git installed
- Basic knowledge of command-line operations and/or your favorite llm chatbot to guide you along


>[!WARNING]
> The Central Server interface is currently in development, please use the Local Interface for now. We will update this guide with instructions for the new interface when it is production ready.

Clone the Bettensor repository and install the required dependencies:

```bash
git clone https://github.com/bettensor/bettensor.git
cd bettensor
pip install -r requirements.txt
pip install -e .
```

## Miner Setup

1. Navigate to the Bettensor directory:
   ```bash
   cd path/to/bettensor
   ```

2. Run the start neuron script:
   ```bash
   source ./scripts/start_neuron.sh
   ```

3. Follow the prompts to set up your miner. You can also use command-line flags to skip the prompts:
   ```bash
   source ./scripts/start_neuron.sh --netuid 1 --subtensor.network test --wallet.name mywallet --wallet.hotkey myhotkey
   ```

>[!NOTE]
> We recommend running with --logging.trace while we are in Beta. This is much more verbose, but it will help us to pinpoint and fix any issues more quickly.





## Choosing Your Interface

When setting up your miner, you'll be prompted to choose between two interface options:

### Local Interface

The Local Interface runs on your local machine and is not accessible from the internet.

**Choose Local Interface (cli.py) if:**
- You prioritize privacy and want to keep your miner isolated from external connections.
- You're comfortable with a more basic user interface and don't need advanced features.
- You prefer to manage your miner(s) directly on your local machine.

### Central Server
NOTE: Currently disabled feature
The Central Server option connects your miner to our web dashboard, allowing for remote management and access to advanced features.

**Choose Central Server if:**
- You want a more user-friendly and feature-rich experience.
- You're comfortable with your miner connecting to our secure central server.
- You want to access and manage your miner(s) from any device through our web dashboard.
- You're interested in more comprehensive data analysis and visualization tools.

>[!IMPORTANT]
> Feature is currently disabled. 



## Running Your Miner

After setup, your miner will start automatically. You can check the logs to ensure it's running correctly:

```bash
pm2 logs miner0
```

Wait for some game data to be received before proceeding to submit predictions. 

## Minimum Stake Requirement

Bettensor now implements a minimum stake requirement for miners to receive weight from validators. This requirement helps ensure that participants are committed to the network and helps prevent abuse.

### Why Stake is Required

1. **Network Security**: Minimum stake requirements protect the network from sybil attacks and other forms of abuse.
2. **Quality Assurance**: Staking demonstrates commitment to the network's success and incentivizes miners to provide quality predictions.
3. **Alignment of Incentives**: Miners with stake in the network are incentivized to act in ways that benefit the network as a whole.

### Checking Your Current Stake

You can check your current stake level on taostats, or with the following command:

```bash
btcli stake show --netuid 30 --wallet.name <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY>
```

### Adding Stake to Your Hotkey

You can add stake to your hotkey using the btcli command:

#### Adding stake to a single hotkey:
```bash
btcli stake add --amount .3 --netuid 30 --wallet.name <YOUR_COLDKEY> --wallet.hotkey <YOUR_HOTKEY>
```

#### Adding stake to multiple hotkeys simultaneously:
```bash
btcli stake add --amount .6 --netuid 30 --wallet.name <YOUR_COLDKEY> --wallet.hotkey hotkey1,hotkey2
```

### Important Notes on Staking

- The minimum stake amount is determined by validators and may change over time.
- Miners without sufficient stake will receive **zero weight** in the network, regardless of their performance or tier.
- Your stake level is checked during each weight calculation cycle.
- Stake can be earned through incentive mechanisms or manually added as shown above.
- You can stake any amount, but only miners meeting the minimum threshold will receive weight.
- Newly registered miners will **always** need to manually add stake to begin with, otherwise they will never receive incentive


>[!IMPORTANT]
>Even if you are in a higher tier and have excellent performance metrics, you will not receive any weight if you do not meet the minimum stake requirement. Make sure to regularly check your stake levels, especially after any network updates.

## Submitting Predictions

#### 1. Command-Line Interface (CLI)

For direct CLI access:

1. SSH into your VPS.

2. Navigate to the Bettensor directory:

   ```bash
   cd path/to/bettensor
   ```

3. Activate the virtual environment:

   ```bash
   source .venv/bin/activate
   ```

4. Run the CLI:

   ```bash
   python bettensor/miner/cli.py
   ```

5. Follow the prompts to submit predictions or perform other actions.

Choose the method that best suits your needs and comfort level. The web interface provides a more user-friendly experience, while the CLI offers direct control.




If you chose the Central Server option, log in to our [web dashboard](https://bettensor.com/dashboard) to connect your miner and submit predictions. You'll need to sign a token generated from the website to connect your miner to the central server. You can access the signing utility from `python bettensor/miner/menu.py` -> option 3 (Sign Token)



## Model Predictions

**DISCLAIMER**: This is the first iteration of the model, you may be at risk of deregistration if you have the model predict using your entire wager limit of $1000, and its bets happen to turn out poor.

**You need to restart your miner after toggling model predictions on/off or after changing the model parameters**

**Version 1.0** introduces the first PyTorch model for automatic soccer game predictions and wager betting, this an optional setting of course. Podos is a small baseline transformer model trained on 100,000 UEFA games and 569 teams, which covers most UEFA teams. Some teams and leagues are not available yet, but will be coming soon.

- Podos will only make bets on soccer games, MLB and more sports will be implemented in the future.
- Model predictions, if toggled on will run once every three hours. 

We have set up a toggle to pick between model and manual predictions, with the toggle set to "on", Podos will be automatically loaded from HuggingFace, predict the game outcome based on average team stats combined with its own historical understanding of game outcomes, and place bets based on its confidence of the outcome. 

Details about the model can be found at our [HuggingFace](https://huggingface.co/Bettensor/podos_soccer_model) repository. We encourage you to download the model, train, modify, or improve it.
Feel free to tag us if you make an improvement or change to Podos you're excited about!

1. To use this model for soccer predictions, and set parameters, you have two options:
```bash
python bettensor/miner/menu.py
```
or direct access:
```bash
python bettensor/miner/interfaces/miner_parameter_tui.py
```
Then restart your miner

**Parameters available to tweak (restart your miner after changing)**:
- Model predictions - toggle model predictions on or off
- Wager distribution steepness - determines the steepness of the sigmoid curve
- Fuzzy match percentage - determines strength of matching similar team names (used to fix non-standardized team names, recommended to stay at 80)
- Minimum wager amount - minimum amount that the model will bet with
- Maximum wager amount - max amount model will bet with in total
- Top N games - maximum number of games the model will bet on with its maximum amount, model may bet on less depending on number of games occuring, how many teams playing it has trained on.

**Tips for Usage**
- The settings can, and should be tweaked to match your desired automation and wager risk. 
- This is the first version of the model, some bias towards specific outcomes might present itself. 
- Start with a smaller maximum wager limit to leave youself room to place your own bets, and increase from there if you find the model to be performing to your liking.
- Podos by default bets with a maximum daily wager amount of $100. This will leave you with $900 available to manually bet with. Increase this value if you would like the model to control more of your daily wager limit.
- Podos will predict on up to the top N number of games it is confident on, the value of N can be tweaked. The amount it bets will sum to the maximum daily wager amount. Namely, if the model predicts on N=5 games and has a max wager limit of $100, the individual bets will sum to the total $100.
- Podos uses a sigmoid curve to allocate larger bets to games it is more confident on, the slope of this curve can be tuned with wager distribution steepness. Higher values will result in larger bets on confident games.
- Model predictions will be skipped for any team the model has not been trained on, and if your current wager allowance is less than the minimum wager amount setting. For any game the model skipped, you can still manually place bets assuming you have enough to place those bets.
- Use a smaller wager distribution steepness value to tame how big the bets are for very confident games, use a larger value if you want a bigger difference between games the model is confident on, and ones that the model is unsure about. Typical ranges for this parameter are between 1 and 20.
- Change the minimum wager amount to set the absolute smallest bet that the model could make. More often than not it will make bets larger than this value. This behavior depends on the number of games to bet on, maximum wager size, and the steepness parameter. Ensure that you are not setting this value larger than the maximum wager amount.
- Tweak the N number of games to control how many games the model will distribute its total maximum wager amount to. A smaller number of games will increase the size of the individual bets. If the number of upcoming games is less than the number of games N, the model will predict on all of the upcoming games, or less.
- We strongly recommend leaving the fuzzy match percentage at the default 80, this parameter is there to resolve any differences in the team names from the data the model was trained on, and team names from the API.




## Managing Multiple Miners

>[!IMPORTANT]
> You can run multiple miners on the same machine, but you will need to ensure that each miner has a unique port. The default port is 12345, so if you run a second miner, you should use port 12346, and so on. Additionally, each miner instance uses a lot of simultaneous database connections, so there are practical limits to how many miners you can run on the same machine. We recommend a maximum of 3 miners on a 6 core machine with 32GB of RAM.

### Local Interface

When running multiple miners locally, you can switch between them in the CLI, there will be a slight delay as the application restarts.



### Central Server

When using the Central Server option, you can manage multiple miners directly from the web dashboard without needing to specify UIDs manually.
- In order to connect your miners to the central server, you'll need to ensure that the `flask-server` pm2 process is running. This can be selected in the `start_neuron.sh` script: choose the `Central Server` option when starting your miner.
- Next, you'll need to sign a token generated from the website to connect your miner to the central server. You can access the signing utility from `python bettensor/miner/menu.py` -> option 3 (Sign Token). or directly "

## Security Considerations

- The Local Interface provides an additional layer of security by running only on your local machine.
- The Central Server option uses secure connections (SSL/TLS) and authentication tokens to protect your data.
- Always use the latest version of the software and keep your system updated.
- Be cautious when using public or unsecured networks, especially when accessing the web dashboard.

## Troubleshooting

If you encounter issues:

1. Check the logs for error messages:
   ```bash
   pm2 logs <miner process name>
   ```

2. Ensure your system meets all prerequisites and dependencies are correctly installed.

3. Verify your network connection and firewall settings, especially if using the Central Server option.

4. If problems persist, please open an issue on our GitHub repository or contact our support team.

## Frequently Asked Questions

**Q: Can I switch between Local Interface and Central Server after initial setup?**

A: Yes, as long as you have started the miner with the flask server running, you can switch between the two interfaces freely. If the flask server is not running, you will need to restart the miner in order to switch interfaces.

**Q: Is my data safe when using the Central Server option?**

A: We employ industry-standard security measures to protect your data. However, as with any internet-connected service, there's always a small risk. Choose the option you're most comfortable with.

**Q: How often should I submit predictions?**

A: The frequency of predictions can vary based on network activity and your strategy. Monitor the network and adjust accordingly.

**Q: Can I run miners on multiple machines?**

A: Yes, you can run miners on different machines. Each miner will need its own wallet and hotkey.

For more questions or support, please visit our official Bettensor Server on [Discord](https://discord.gg/qj7UzV8Cxd) 
or on the official [Bittensor Discord, channel #30](https://discord.gg/bittensor).


## Troubleshooting

Here are some common issues miners might encounter and how to resolve them:

1. **Port not open on machine**
   - Ensure the required ports are open on your machine.
   - If using UFW (Uncomplicated Firewall), open the necessary port:
     ```
     sudo ufw allow <port_number>
     ```
   - Check if your VPS provider has an external firewall. You may need to configure it in your provider's dashboard.

2. **Checking logs**
   - To view logs for all processes:
     ```
     pm2 log
     ```
   - To view logs for a specific process:
     ```
     pm2 log <process_id>
     ```

3. **Restarting processes**
   - To restart all processes:
     ```
     pm2 restart all
     ```
   - To restart a specific process:
     ```
     pm2 restart <process_id>
     ```

4. **Connection issues**
   - Ensure your internet connection is stable.
   - Check if the subtensor endpoint is accessible.

5. **API key issues**
   - Verify that your API key is correctly set in the `.env` file.
   - Ensure you have sufficient credits on your RapidAPI account.

6. **Unexpected behavior**
   - Try stopping all processes:
     ```
     pm2 stop all
     ```
   - Then start them again:
     ```
     pm2 start all
     ```

If you continue to experience issues, please reach out to the community support channels for further assistance.

## Database Setup

Bettensor uses PostgreSQL as its database. The system is designed to work with both root and non-root users, but using the root user provides full functionality, especially during initial setup.

### Database Configuration

By default, the system attempts to connect to the database using the following configuration:

- Host: localhost
- Port: 5432
- Database Name: bettensor
- User: root
- Password: bettensor_password

You can override these settings using environment variables:

- DB_HOST
- DB_PORT
- DB_NAME
- DB_USER
- DB_PASSWORD

### Root User Privileges

While the system can operate with non-root users, using the root user ensures that all operations, including database and table creation, can be performed without issues.

If you're using a non-root user:
1. Ensure the database exists before running the miner.
2. Grant necessary permissions to the user for the bettensor database.

### Setting Up PostgreSQL Root User

If you haven't set up a root user in PostgreSQL, follow these steps:

1. Switch to the postgres system user:
   ```
   sudo -i -u postgres
   ```

2. Access the PostgreSQL prompt:
   ```
   psql
   ```

3. Create a new superuser named 'root':
   ```sql
   CREATE USER root WITH SUPERUSER PASSWORD 'your_secure_password';
   ```

4. Exit the PostgreSQL prompt:
   ```
   \q
   ```

5. Update your .env file or environment variables with the new root user credentials.

>[!IMPORTANT]
> Always use a strong, unique password for your database root user. Never share this password or commit it to version control systems.

### Database Initialization

The DatabaseManager class handles database initialization:
- It checks if the specified database exists.
- If the database doesn't exist and the user has root privileges, it creates the database.
- It creates necessary tables if they don't exist.

If you encounter any database-related issues during setup or operation, check the logs for specific error messages and ensure your database configuration is correct.

