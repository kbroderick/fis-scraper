"""reinitialize_database

Revision ID: reinitialize_database
Revises: 
Create Date: 2024-03-19 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = 'reinitialize_database'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create enum types if they don't exist
    conn = op.get_bind()
    
    # Drop existing tables if they exist
    op.drop_table('athlete_points', if_exists=True)
    op.drop_table('race_results', if_exists=True)
    op.drop_table('points_lists', if_exists=True)
    op.drop_table('athletes', if_exists=True)
    
    # Drop existing enum types if they exist
    conn.execute(text('DROP TYPE IF EXISTS gender CASCADE'))
    conn.execute(text('DROP TYPE IF EXISTS discipline CASCADE'))
    
    # Create enum types
#    conn.execute(text("CREATE TYPE gender AS ENUM ('M', 'F')"))
 #   conn.execute(text("CREATE TYPE discipline AS ENUM ('SL', 'GS', 'SG', 'DH')"))
    
    # Create athletes table
    op.create_table('athletes',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('fis_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('country', sa.String(), nullable=False),
        sa.Column('nation_code', sa.CHAR(length=3), nullable=False),
        sa.Column('gender', postgresql.ENUM('M', 'F', name='gender'), nullable=False),
        sa.Column('birth_date', sa.Date(), nullable=True),
        sa.Column('birth_year', sa.Integer(), nullable=True),
        sa.Column('ski_club', sa.String(), nullable=True),
        sa.Column('national_code', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('fis_id')
    )
    
    # Create points_lists table
    op.create_table('points_lists',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('publication_date', sa.Date(), nullable=False),
        sa.Column('valid_from', sa.Date(), nullable=False),
        sa.Column('valid_to', sa.Date(), nullable=False),
        sa.Column('season', sa.String(), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create race_results table
    op.create_table('race_results',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('athlete_id', sa.Integer(), nullable=False),
        sa.Column('race_date', sa.Date(), nullable=False),
        sa.Column('discipline', postgresql.ENUM('SL', 'GS', 'SG', 'DH', name='discipline'), nullable=False),
        sa.Column('points', sa.Float(), nullable=True),
        sa.Column('rank', sa.Integer(), nullable=True),
        sa.Column('race_name', sa.String(), nullable=True),
        sa.Column('location', sa.String(), nullable=True),
        sa.Column('win_time', sa.Float(), nullable=True),
        sa.Column('racer_time', sa.Float(), nullable=True),
        sa.Column('penalty', sa.Float(), nullable=True),
        sa.Column('race_points', sa.Float(), nullable=True),
        sa.Column('race_category', sa.String(), nullable=True),
        sa.Column('total_starters', sa.Integer(), nullable=True),
        sa.Column('total_finishers', sa.Integer(), nullable=True),
        sa.Column('race_codex', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(['athlete_id'], ['athletes.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create athlete_points table
    op.create_table('athlete_points',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('athlete_id', sa.Integer(), nullable=False),
        sa.Column('points_list_id', sa.Integer(), nullable=False),
        sa.Column('sl_points', sa.Float(), nullable=True),
        sa.Column('gs_points', sa.Float(), nullable=True),
        sa.Column('sg_points', sa.Float(), nullable=True),
        sa.Column('dh_points', sa.Float(), nullable=True),
        sa.Column('sl_rank', sa.Integer(), nullable=True),
        sa.Column('gs_rank', sa.Integer(), nullable=True),
        sa.Column('sg_rank', sa.Integer(), nullable=True),
        sa.Column('dh_rank', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['athlete_id'], ['athletes.id'], ),
        sa.ForeignKeyConstraint(['points_list_id'], ['points_lists.id'], ),
        sa.PrimaryKeyConstraint('id')
    )


def downgrade() -> None:
    # Drop all tables
    op.drop_table('athlete_points')
    op.drop_table('race_results')
    op.drop_table('points_lists')
    op.drop_table('athletes')
    
    # Drop enum types
    conn = op.get_bind()
    conn.execute(text('DROP TYPE IF EXISTS gender CASCADE'))
    conn.execute(text('DROP TYPE IF EXISTS discipline CASCADE')) 