use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType},
    identifiers::ClientId,
    types::Currency,
};

use crate::{
    common::consts::LIGHTPOOL,
    config::{LightpoolDataClientConfig, LightpoolExecClientConfig},
    data::LightpoolDataClient,
    execution::{LightpoolExecutionClient, default_account_id},
};

impl ClientConfig for LightpoolDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct LightpoolDataClientFactory;

impl DataClientFactory for LightpoolDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let lightpool_config = config
            .as_any()
            .downcast_ref::<LightpoolDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config for LightpoolDataClientFactory, expected LightpoolDataClientConfig"
                )
            })?
            .clone();
        let client_id = ClientId::from(name);
        Ok(Box::new(LightpoolDataClient::new(
            client_id,
            lightpool_config,
        )))
    }

    fn name(&self) -> &'static str {
        LIGHTPOOL
    }

    fn config_type(&self) -> &'static str {
        "LightpoolDataClientConfig"
    }
}

impl ClientConfig for LightpoolExecClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct LightpoolExecutionClientFactory;

impl ExecutionClientFactory for LightpoolExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        let lightpool_config = config
            .as_any()
            .downcast_ref::<LightpoolExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config for LightpoolExecutionClientFactory, expected LightpoolExecClientConfig"
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let account_id = default_account_id();
        let core = ExecutionClientCore::new(
            nautilus_model::identifiers::TraderId::from("LIGHTPOOL-TRADER"),
            client_id,
            *crate::common::consts::LIGHTPOOL_VENUE,
            OmsType::Netting,
            account_id,
            AccountType::Cash,
            Some(Currency::from(crate::common::consts::LPUSD)),
            cache,
        );
        Ok(Box::new(LightpoolExecutionClient::new(core, lightpool_config)?))
    }

    fn name(&self) -> &'static str {
        LIGHTPOOL
    }

    fn config_type(&self) -> &'static str {
        "LightpoolExecClientConfig"
    }
}
