# Power Markets Pipeline - ERCOT Data Processing

A high-performance Rust-based data processing pipeline for ERCOT (Electric Reliability Council of Texas) market data, specializing in Battery Energy Storage System (BESS) analysis and revenue calculations.

## Overview

This pipeline processes multiple ERCOT data sources including:
- Real-time Settlement Point Prices
- Day-Ahead Market (DAM) data
- Ancillary Services data
- Locational Marginal Prices (LMPs)
- 60-Day Disclosure Reports
- BESS resource identification and analysis

## Features

- **Parallel Processing**: Utilizes all available CPU cores with Rayon
- **Multiple Output Formats**: CSV, Apache Parquet, and Apache Arrow
- **BESS Analysis**: Identifies and tracks 273+ battery storage resources
- **High Performance**: Processes millions of records in seconds
- **Memory Efficient**: Streaming processing for large datasets

## Installation

### Prerequisites

- Rust 1.70+ (install from [rustup.rs](https://rustup.rs))
- 8GB+ RAM recommended
- Multi-core CPU for optimal performance

### Build

```bash
cargo build --release
```

## Usage

### Process Real-Time Settlement Point Prices
```bash
./target/release/rt_rust_processor
```

### Process Day-Ahead Market Data
```bash
./target/release/rt_rust_processor --dam
```

### Process Ancillary Services Data
```bash
./target/release/rt_rust_processor --ancillary
```

### Process LMP Data
```bash
./target/release/rt_rust_processor --lmp        # Full extraction
./target/release/rt_rust_processor --lmp-fast   # Process existing CSVs
./target/release/rt_rust_processor --lmp-all    # Process all historical data
```

### Process 60-Day Disclosure Reports
```bash
./target/release/rt_rust_processor --disclosure      # Full extraction
./target/release/rt_rust_processor --disclosure-fast # Process existing CSVs
```

### Analyze BESS Resources
```bash
./target/release/rt_rust_processor --bess
```

## Data Processing Modules

### 1. Real-Time Processor (`main.rs`)
Processes ERCOT real-time settlement point prices with automatic year detection and parallel processing.

### 2. DAM Processor (`dam_processor.rs`)
Handles Day-Ahead Market settlement data with ZIP extraction and annual rollups.

### 3. Ancillary Services Processor (`ancillary_processor.rs`)
Processes multiple ancillary service types:
- ECRS (ERCOT Contingency Reserve Service)
- REGDN (Regulation Down)
- REGUP (Regulation Up)
- RRS (Responsive Reserve Service)
- Non-Spinning Reserves

### 4. LMP Processors (`lmp_processor.rs`, `lmp_fast_processor.rs`)
- Handles nested ZIP extraction (ZIP → ZIP → CSV)
- Processes 870,000+ CSV files efficiently
- Creates annual rollups by Resource Nodes, Load Zones, and Trading Hubs

### 5. Disclosure Processors (`disclosure_processor.rs`, `disclosure_fast_processor.rs`)
Processes 60-Day delayed disclosure reports:
- SCED (Security Constrained Economic Dispatch)
- DAM (Day-Ahead Market)
- COP (Current Operating Plan)
- SASM (Self-Arranged Ancillary Services Market)

### 6. BESS Analyzer (`bess_analyzer.rs`)
- Identifies all BESS resources (273+ unique systems)
- Tracks capacity, QSE, DME, and settlement points
- Analyzes operational status patterns
- Prepares data for revenue calculations

## Performance Benchmarks

- **RT Settlement Prices**: 32,225 files in 14.4 seconds
- **LMP Data**: 873,434 CSV files creating 526M+ records
- **Compression**: 96% reduction with Parquet format
- **Parallel Efficiency**: Near-linear scaling with CPU cores

## Output Formats

All processors support three output formats:

1. **CSV**: Human-readable, compatible with Excel/spreadsheets
2. **Parquet**: Compressed columnar format (96% compression)
3. **Arrow**: High-performance columnar format for analytics

## Architecture

The pipeline uses a modular architecture with:
- Trait-based processor design for extensibility
- Streaming processing for memory efficiency
- Parallel batch processing with progress indicators
- Error resilience with partial failure handling

## BESS Analysis Insights

The BESS analyzer identified:
- **273 unique BESS resources** in ERCOT
- Largest facilities: 240 MW (ANOL_ESS_BES1)
- Most active resources: 10,000+ operational records
- Status distribution: 82% ON, 17% OUT/ONTEST, 1% OFF

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run `cargo fmt` and `cargo clippy`
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Built with [Polars](https://pola.rs/) for high-performance data processing
- Uses [Rayon](https://github.com/rayon-rs/rayon) for parallel processing
- Inspired by Modo Energy's BESS benchmarking methodology