# bettensor/validator/database/schema.py
from sqlalchemy import (
    MetaData, Table, Column, Integer, Text, REAL, TIMESTAMP, BOOLEAN,
    PrimaryKeyConstraint, ForeignKey, UniqueConstraint, CheckConstraint, Index,
    ForeignKeyConstraint,
    JSON, # Keep for potential other uses, though JSONB is preferred for PG
    Float, # Use Float for better precision than REAL if needed
    func, Identity, String
)
from sqlalchemy.dialects.postgresql import JSONB # Import JSONB for PostgreSQL

# Create a MetaData instance
# Using a naming convention for constraints is recommended for consistency
# https://alembic.sqlalchemy.org/en/latest/naming.html
naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
metadata = MetaData(naming_convention=naming_convention)

# --- Define Tables (Using Appropriate PG Types) ---

db_version = Table(
    'db_version', metadata,
    Column('version', Integer, primary_key=True),
    Column('updated_at', TIMESTAMP(timezone=True), server_default=func.now())
)

score_state = Table(
    'score_state', metadata,
    Column('state_id', Integer, Identity(), primary_key=True),
    Column('current_day', Integer, nullable=False),
    Column('current_date', TIMESTAMP(timezone=True), nullable=False),
    Column('reference_date', TIMESTAMP(timezone=True), nullable=False),
    Column('invalid_uids', Text), # Storing list/JSON as TEXT
    Column('valid_uids', Text),   # Storing list/JSON as TEXT
    Column('last_update_date', TIMESTAMP(timezone=True)),
    Column('amount_wagered', Text), # Storing serialized data as TEXT
    Column('tiers', Text)           # Storing serialized data as TEXT
)

miner_stats = Table(
    'miner_stats', metadata,
    Column('miner_uid', Integer, primary_key=True),
    Column('miner_hotkey', Text, unique=True),
    Column('miner_coldkey', Text),

    #Current Miner Stats (most recent daily values)
    Column('miner_rank', Integer, server_default='0'),
    Column('miner_status', Text, server_default='active'),
    Column('miner_cash', Float, server_default='0.0'), # Use Float for precision
    Column('miner_current_tier', Integer, server_default='1'),
    Column('miner_current_scoring_window', Integer, server_default='0'),

    # Current Daily Miner Scores 
    Column('miner_current_composite_score', Float, server_default='0.0'),
    Column('miner_current_entropy_score', Float, server_default='0.0'),
    Column('miner_current_sharpe_ratio', Float, server_default='0.0'),
    Column('miner_current_sortino_ratio', Float, server_default='0.0'),
    Column('miner_current_roi', Float, server_default='0.0'),
    Column('miner_current_clv_avg', Float, server_default='0.0'),
    Column('miner_last_prediction_date', TIMESTAMP(timezone=True)), # Use TIMESTAMP
    Column('most_recent_weight', Float, server_default='0.0'), # weight set by validator
    Column('miner_current_incentive', Float, server_default='0.0'), # incentive set after consensus between all validators

    #Current Tier Miner Scores (tier avg from whatever tier the miner is in)
    Column('miner_tier_composite_score', Float, server_default='0.0'),
    Column('miner_tier_entropy_score', Float, server_default='0.0'),
    Column('miner_tier_sharpe_ratio', Float, server_default='0.0'),
    Column('miner_tier_sortino_ratio', Float, server_default='0.0'),
    Column('miner_tier_roi', Float, server_default='0.0'),
    Column('miner_tier_clv_avg', Float, server_default='0.0'),

    # Daily Impacts on Tier Scores (will the daily score add or subtract from the tier score?)
    Column('miner_daily_composite_score_impact', Float, server_default='0.0'),
    Column('miner_daily_entropy_score_impact', Float, server_default='0.0'),
    Column('miner_daily_sharpe_ratio_impact', Float, server_default='0.0'),
    Column('miner_daily_sortino_ratio_impact', Float, server_default='0.0'),
    Column('miner_daily_roi_impact', Float, server_default='0.0'),
    Column('miner_daily_clv_avg_impact', Float, server_default='0.0'),

    # Recent Average Stats
    Column('miner_3_day_roi', Float, server_default='0.0'),
    Column('miner_7_day_roi', Float, server_default='0.0'),
    Column('miner_15_day_roi', Float, server_default='0.0'),
    Column('miner_30_day_roi', Float, server_default='0.0'),
    Column('miner_45_day_roi', Float, server_default='0.0'),

    # Miner to beat stats (miner_uid of miner to beat, plus their tier scores) - this is the next higher ranked miner, either in the same tier or the next tier up
    Column('miner_to_beat_uid', Integer, server_default='0'),
    Column('miner_to_beat_tier', Integer, server_default='0'),
    Column('miner_to_beat_composite_score', Float, server_default='0.0'),
    Column('miner_to_beat_entropy_score', Float, server_default='0.0'),
    Column('miner_to_beat_sharpe_ratio', Float, server_default='0.0'),
    Column('miner_to_beat_sortino_ratio', Float, server_default='0.0'),
    Column('miner_to_beat_roi', Float, server_default='0.0'),
    Column('miner_to_beat_clv_avg', Float, server_default='0.0'),

    # Lifetime Miner Stats
    Column('miner_lifetime_earnings', Float, server_default='0.0'),
    Column('miner_lifetime_wager_amount', Float, server_default='0.0'),
    Column('miner_lifetime_roi', Float, server_default='0.0'),
    Column('miner_lifetime_predictions', Integer, server_default='0'),
    Column('miner_lifetime_wins', Integer, server_default='0'),
    Column('miner_lifetime_losses', Integer, server_default='0'),
    Column('miner_win_loss_ratio', Float, server_default='0.0'),
    
)

game_data = Table(
    'game_data', metadata,
    Column('game_id', Text, primary_key=True),
    Column('external_id', Integer, unique=True),
    Column('team_a', Text),
    Column('team_b', Text),
    # Old Moneyline columns, will be deprecated soon in favor of lines dictionary column
    Column('team_a_odds', Float), # Deprecate soon
    Column('team_b_odds', Float), # Deprecate soon
    Column('tie_odds', Float), # Deprecate soon,

    # Use JSONB for lines dictionary
    Column('lines', JSONB, nullable=True), # dictionary of lines and odds offered by the sportsbooks

    Column('can_tie', BOOLEAN), # Use BOOLEAN
    Column('event_start_date', TIMESTAMP(timezone=True)),
    Column('create_date', TIMESTAMP(timezone=True), server_default=func.now()),
    Column('last_update_date', TIMESTAMP(timezone=True), onupdate=func.now()),
    Column('sport', Text),
    Column('league', Text),
    Column('outcome', Integer, server_default='3'), #final outcome, 0 = team_a, 1 = team_b, 2 = tie, 3 = unfinished

    # Use JSONB for outcomes dictionary
    Column('outcomes', JSONB, nullable=True),

    Column('active', BOOLEAN, server_default='true') # Use BOOLEAN
)

keys = Table(
    'keys', metadata,
    Column('hotkey', Text, primary_key=True),
    Column('coldkey', Text)
)

predictions = Table(
    'predictions', metadata,
    Column('prediction_id', Text, primary_key=True),
    Column('miner_uid', Integer, nullable=False),
    Column('miner_hotkey', Text, nullable=False),
    Column('game_id', Integer, nullable=False),
    Column('prediction_date', TIMESTAMP(timezone=True), nullable=False),
    Column('team_a', Text, nullable=True),
    Column('team_b', Text, nullable=True),
    Column('team_a_odds', Float, nullable=True),
    Column('team_b_odds', Float, nullable=True),
    Column('tie_odds', Float, nullable=True),
    Column('model_name', Text, nullable=True),
    Column('confidence_score', Float, nullable=True),
    Column('prediction_type', Text, nullable=True),
    Column('predicted_outcome', Integer, nullable=True),
    Column('predicted_odds', Float, nullable=True),
    Column('wager', Float, nullable=False),
    Column('payout', Float),
    Column('outcome', Integer, nullable=True),
    Column('sent_to_site', Integer, server_default='0'),
)

entropy_game_pools = Table(
    'entropy_game_pools', metadata,
    Column('game_id', Integer),
    Column('outcome', Integer),
    Column('pool_size', Float), # Use Float - Restored from PG schema version
    Column('entropy_score', Float, server_default='0.0'), # Use Float
    PrimaryKeyConstraint('game_id', 'outcome', name='pk_entropy_game_pools')
)

entropy_predictions = Table(
    'entropy_predictions', metadata,
    Column('prediction_id', Text, primary_key=True),
    Column('game_id', Integer),
    Column('outcome', Integer),
    Column('miner_uid', Integer),
    Column('miner_hotkey', Text),
    Column('odds', Float), # Use Float
    Column('wager', Float), # Use Float
    Column('prediction_date', TIMESTAMP(timezone=True)),
    Column('entropy_contribution', Float), # Use Float
    ForeignKeyConstraint(['game_id', 'outcome'],
                         ['entropy_game_pools.game_id', 'entropy_game_pools.outcome'],
                         name='fk_entropy_predictions_game_outcome')
)

entropy_miner_scores = Table(
    'entropy_miner_scores', metadata,
    Column('miner_uid', Integer),
    Column('day', Integer),
    Column('contribution', Float), # Use Float
    PrimaryKeyConstraint('miner_uid', 'day', name='pk_entropy_miner_scores')
)

entropy_system_state = Table(
    'entropy_system_state', metadata,
    Column('id', Integer, primary_key=True, server_default='1'), # Keep default 1
    Column('current_day', Integer, server_default='0'), # Add default back
    Column('num_miners', Integer),
    Column('max_days', Integer),
    Column('last_processed_date', TIMESTAMP(timezone=True)),
    CheckConstraint('id = 1', name='ck_entropy_system_state_id')
)

scores = Table(
    'scores', metadata,
    Column('score_id', Integer, Identity(), primary_key=True),
    Column('miner_uid', Integer, nullable=False),
    Column('miner_hotkey', Text, nullable=False),
    Column('day_id', Integer, nullable=False),
    Column('score_type', String, nullable=False),
    Column('clv_score', Float, nullable=True, server_default='0.0'),
    Column('roi_score', Float, nullable=True, server_default='0.0'),
    Column('sortino_score', Float, nullable=True, server_default='0.0'),
    Column('entropy_score', Float, nullable=True, server_default='0.0'),
    Column('composite_score', Float, nullable=True, server_default='0.0'),
    UniqueConstraint('miner_uid', 'day_id', 'score_type', 'miner_hotkey', name='uq_scores_miner_day_type_hotkey'),
    ForeignKeyConstraint(['miner_uid'], ['miner_stats.miner_uid'], name='fk_scores_miner_uid')
)

entropy_closed_games = Table(
    'entropy_closed_games', metadata,
    Column('game_id', Integer, primary_key=True),
    Column('close_time', TIMESTAMP(timezone=True))
)

# Optional: Add Indexes for commonly queried columns
Index('ix_predictions_miner_uid_date', predictions.c.miner_uid, predictions.c.prediction_date)
Index('ix_predictions_miner_hotkey_date', predictions.c.miner_hotkey, predictions.c.prediction_date)
Index('ix_entropy_predictions_miner_uid_date', entropy_predictions.c.miner_uid, entropy_predictions.c.prediction_date)
Index('ix_game_data_event_start_date', game_data.c.event_start_date) 