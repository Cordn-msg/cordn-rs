//! The rmcp server: thin glue that extracts the injected caller pubkey, request
//! event id, and (for streaming tools) the `OpenStreamWriter` from each
//! request's `RequestContext`, then delegates to [`CoordinatorAdapter`]. Ports
//! the tool registration in `references/cordn/src/server/coordinatorMethods.ts`.
#![cfg(feature = "server")]

use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, ErrorData, Implementation, ServerCapabilities},
    schemars,
    service::{RequestContext, RoleServer},
    tool, tool_handler, tool_router, ServerHandler,
};
use serde::Serialize;

use async_trait::async_trait;
use contextvm_sdk::transport::open_stream::OpenStreamWriter;
use contextvm_sdk::transport::server::{ClientPubkey, InboundEvent};

use cordn_core::contracts::{
    ConsumeKeyPackageInput, FetchGroupMessagesInput, FetchManyGroupMessagesInput,
    FetchManyPendingJoinRequestsInput, FetchPendingJoinRequestsInput, FetchPendingWelcomesInput,
    NostrEvent, PostGroupMessageInput, PublishKeyPackageInput, RemoveKeyPackagesInput,
    StoreJoinRequestInput, StoreWelcomeInput, SubscribeGroupMessagesInput,
    SubscribeManyGroupMessagesInput,
};

use crate::adapter::{AdapterError, CoordinatorAdapter, MessageSink};

/// The rmcp server. Clone-cheap (everything is behind an `Arc`).
#[derive(Clone)]
pub struct CordnServer {
    adapter: std::sync::Arc<CoordinatorAdapter>,
}

impl CordnServer {
    pub fn new(adapter: std::sync::Arc<CoordinatorAdapter>) -> Self {
        Self { adapter }
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct EmptyInput {}

/// Adapts the rmcp `OpenStreamWriter` to the adapter's [`MessageSink`] trait.
struct StreamWriter(OpenStreamWriter);

#[async_trait]
impl MessageSink for StreamWriter {
    async fn start(&self) -> bool {
        self.0.start().await.is_ok()
    }
    async fn write(&self, msg: String) -> bool {
        self.0.write(msg).await.is_ok() && self.0.is_active()
    }
    fn is_active(&self) -> bool {
        self.0.is_active()
    }
    async fn close(&self) {
        let _ = self.0.close().await;
    }
}

fn client_pubkey(ctx: &RequestContext<RoleServer>) -> Result<String, ErrorData> {
    ctx.extensions
        .get::<ClientPubkey>()
        .map(|c| c.0.clone())
        .ok_or_else(|| ErrorData::invalid_params("Missing injected client pubkey", None))
}

/// The transport injects the full client-signed request event as
/// `InboundEvent(pub nostr::Event)`; convert it to our wire type via a JSON
/// round-trip (both use the canonical Nostr event shape). Required for publish
/// binding and storage — the client `sig` cannot be reconstructed server-side.
fn publication_event(ctx: &RequestContext<RoleServer>) -> Result<NostrEvent, ErrorData> {
    let ev = ctx
        .extensions
        .get::<InboundEvent>()
        .ok_or_else(|| ErrorData::invalid_params("Missing inbound publication event", None))?;
    let value = serde_json::to_value(&ev.0).unwrap_or(serde_json::Value::Null);
    serde_json::from_value::<NostrEvent>(value)
        .map_err(|e| ErrorData::invalid_params(e.to_string(), None))
}

fn structured<T: Serialize>(out: T) -> CallToolResult {
    let mut result = CallToolResult::success(vec![]);
    result.structured_content = Some(serde_json::to_value(out).unwrap_or(serde_json::Value::Null));
    result
}

fn adapter_error(e: AdapterError) -> ErrorData {
    ErrorData::invalid_params(e.to_string(), None)
}

fn stream_writer(ctx: &RequestContext<RoleServer>) -> Result<StreamWriter, ErrorData> {
    ctx.extensions
        .get::<OpenStreamWriter>()
        .cloned()
        .map(StreamWriter)
        .ok_or_else(|| ErrorData::invalid_params("Expected open stream writer", None))
}

#[tool_router]
impl CordnServer {
    #[tool(description = "Publish an MLS key package for the injected caller identity.")]
    async fn kp_publish(
        &self,
        Parameters(input): Parameters<PublishKeyPackageInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let event = publication_event(&ctx)?;
        let out = self
            .adapter
            .publish_key_package(input, &pubkey, Some(event))
            .await
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(
        description = "List currently available published MLS key packages discoverable on the coordinator."
    )]
    async fn kp_list(
        &self,
        Parameters(_): Parameters<EmptyInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .list_available_key_packages(&pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(
        description = "Consume the next published MLS key package by stable identity or exact key package ref."
    )]
    async fn kp_take(
        &self,
        Parameters(input): Parameters<ConsumeKeyPackageInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .consume_key_package(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(
        description = "Remove published MLS key packages owned by the injected caller identity."
    )]
    async fn kp_remove(
        &self,
        Parameters(input): Parameters<RemoveKeyPackagesInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .remove_key_packages(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(description = "Fetch pending welcomes queued for the injected caller identity.")]
    async fn welcome_take(
        &self,
        Parameters(input): Parameters<FetchPendingWelcomesInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .fetch_pending_welcomes(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(description = "Store an MLS welcome for a target stable identity.")]
    async fn welcome_store(
        &self,
        Parameters(input): Parameters<StoreWelcomeInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .store_welcome(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(description = "Store a join request for a group from the injected caller identity.")]
    async fn join_request_store(
        &self,
        Parameters(input): Parameters<StoreJoinRequestInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .store_join_request(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(description = "Fetch pending join requests for a group.")]
    async fn join_request_take(
        &self,
        Parameters(input): Parameters<FetchPendingJoinRequestsInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .fetch_pending_join_requests(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(description = "Fetch pending join requests for multiple groups in a single call.")]
    async fn join_request_take_many(
        &self,
        Parameters(input): Parameters<FetchManyPendingJoinRequestsInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .fetch_many_pending_join_requests(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(description = "Queue an MLS opaque group message for the injected caller identity.")]
    async fn msg_post(
        &self,
        Parameters(input): Parameters<PostGroupMessageInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .post_group_message(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(description = "Fetch queued MLS opaque group messages by group and optional cursor.")]
    async fn msg_fetch(
        &self,
        Parameters(input): Parameters<FetchGroupMessagesInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .fetch_group_messages(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(
        description = "Fetch queued MLS opaque group messages for multiple groups with independent optional cursors."
    )]
    async fn msg_fetch_many(
        &self,
        Parameters(input): Parameters<FetchManyGroupMessagesInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let out = self
            .adapter
            .fetch_many_group_messages(input, &pubkey)
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(
        description = "Replay and stream MLS opaque group messages by group and optional cursor."
    )]
    async fn msg_sub(
        &self,
        Parameters(input): Parameters<SubscribeGroupMessagesInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let sink = stream_writer(&ctx)?;
        let out = self
            .adapter
            .subscribe_group_messages(input, &pubkey, &sink)
            .await
            .map_err(adapter_error)?;
        Ok(structured(out))
    }

    #[tool(
        description = "Replay and stream MLS opaque group messages for multiple groups with independent optional cursors."
    )]
    async fn msg_sub_many(
        &self,
        Parameters(input): Parameters<SubscribeManyGroupMessagesInput>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let pubkey = client_pubkey(&ctx)?;
        let sink = stream_writer(&ctx)?;
        let out = self
            .adapter
            .subscribe_many_group_messages(input, &pubkey, &sink)
            .await
            .map_err(adapter_error)?;
        Ok(structured(out))
    }
}

#[tool_handler]
impl ServerHandler for CordnServer {
    fn get_info(&self) -> rmcp::model::ServerInfo {
        rmcp::model::ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("cordn-server", env!("CARGO_PKG_VERSION"))
                    .with_title("ContextVM MLS Coordinator"),
            )
            .with_instructions("cordn MLS delivery coordinator")
    }
}
