-- CALC: IsDistributed
-- TRUE when the reward has been distributed to the client (DistributeDate is set).
-- A reward can be settled (liquid) but not yet distributed — distribution requires
-- client instruction or hits an auto-distribute threshold. Used to reconcile
-- the settled pool against what has actually left the custodian ledger.

SELECT
  StakingEvents.ID AS ID
  , StakingEvents.DistributeDate IS NOT NULL AS IsDistributed
FROM {{ source('anchorage_data_platform', 'StakingEvents') }} AS StakingEvents
