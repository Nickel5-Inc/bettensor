# service to fetch the alpha staked on each UID, so we can set a minimum stake requirement

import bittensor as bt
from bittensor import Balance


class MinStakeService:
    """
    Service to fetch the alpha staked on each UID, so we can set a minimum stake requirement
    """
    MIN_STAKE_TAO = Balance(0.3) #fixed for now, could eventually be dynamic like reg fee
    def __init__(self, subtensor: bt.subtensor):
        self.subtensor = subtensor

    def get_min_stake_for_all_uids(self) -> list[bool]:
        """
        Get the minimum stake for all UIDs in the metagraph.
        """
        
        metagraph_info = self.subtensor.get_metagraph_info(netuid=30)
        alpha_stakes : list[Balance] = metagraph_info.alpha_stake
        moving_price = metagraph_info.moving_price
        min_stakes = []
        true_count = 0
        false_count = 0
        for uid,balance in enumerate(alpha_stakes):
            print(f"UID {uid} has {balance.tao} alpha stake")
            print(f"Moving price: {moving_price}")
            print(f"Stake value in TAO: {(balance.tao * moving_price)}")
            print("min stake in TAO:", self.MIN_STAKE_TAO)
            
            if balance.tao * moving_price >= self.MIN_STAKE_TAO:
                print(f"UID {uid} meets the min stake with {balance.tao * moving_price} TAO")
                min_stakes.append(True)
                true_count += 1
            else:
                min_stakes.append(False)
                false_count += 1
        #log percentage of uids that meet the min stake
        print(f"Percentage of UIDs that meet the min stake: {true_count / 256 * 100}%")
        return min_stakes


if __name__ == "__main__":
    subtensor = bt.subtensor(network="finney")
    min_stake_service = MinStakeService(subtensor)
    min_stake_service.get_min_stake_for_all_uids()
