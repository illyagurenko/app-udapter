--liquibase formatted sql

--changeset illya:adapter-015-gru-config
INSERT INTO ACC_APP_ADAPTER.APP_ADAPTER_CONFIG
(
    ID,
    SYSTEMID,
    EVENT_TYPE,
    ENTITY_TYPE,
    TOPIC_IN,
    STATUS_IN,
    TOPIC_OUT,
    STATUS_OUT,
    INS_TS,
    UPD_TS
)
VALUES
(
    ACC_APP_ADAPTER.APP_ADAPTER_CONFIG_SEQ.NEXTVAL,
    'GRU',
    'BALANCE',
    'ACCOUNT',
    '${gruTopicIn}',
    1,
    '${gruTopicOut}',
    1,
    SYSDATE,
    SYSDATE
);

--rollback DELETE FROM ACC_APP_ADAPTER.APP_ADAPTER_CONFIG WHERE SYSTEMID = 'GRU';