// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CloudFetch implementation for streaming Arrow data from cloud storage.
//!
//! This module provides:
//! - `StreamingCloudFetchProvider`: Main orchestrator for downloading and streaming
//! - `ChunkLinkFetcher`: Trait for fetching chunk links from the backend
//! - `ChunkDownloader`: Downloads and parses Arrow data from presigned URLs
//! - Arrow IPC parsing utilities

pub mod arrow_parser;
pub mod chunk_downloader;
pub mod link_fetcher;
pub mod streaming_provider;

pub use arrow_parser::parse_arrow_ipc;
pub use chunk_downloader::ChunkDownloader;
pub use link_fetcher::{ChunkLinkFetcher, SeaChunkLinkFetcher};
pub use streaming_provider::StreamingCloudFetchProvider;
