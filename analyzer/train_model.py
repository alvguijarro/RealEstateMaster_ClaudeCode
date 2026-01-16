#!/usr/bin/env python
"""
=============================================================================
TRAIN MODEL - Standalone CLI for Training Rent Estimation Model
=============================================================================
Train a ML model for rental price estimation using ALQUILER data.

Usage:
    python train_model.py --data alquiler.xlsx --outdir models/
    python train_model.py --data alquiler.xlsx --with_quantiles --use_text

Output:
    - models/rent_model.joblib - Trained model
    - models/metadata.json - Training metadata and metrics
=============================================================================
"""

import argparse
import json
import sys
import os
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
import joblib

from ml_rent_model import (
    train_rent_model,
    prepare_features,
    NUMERIC_FEATURES,
    CATEGORICAL_FEATURES,
    BINARY_FEATURES,
    LEAKAGE_COLUMNS
)


def load_table(path: str) -> pd.DataFrame:
    """
    Load data from Excel (multi-sheet) or CSV file.
    
    Args:
        path: Path to file (.xlsx or .csv)
        
    Returns:
        DataFrame with all data concatenated
    """
    path = Path(path)
    
    if path.suffix.lower() in ['.xlsx', '.xls']:
        print(f"  Loading Excel file: {path}")
        xl = pd.ExcelFile(path)
        dfs = []
        for sheet in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sheet)
            df['_source_sheet'] = sheet
            dfs.append(df)
            print(f"    -> Sheet '{sheet}': {len(df)} rows")
        df = pd.concat(dfs, ignore_index=True)
        print(f"  Total: {len(df)} rows from {len(xl.sheet_names)} sheets")
    else:
        print(f"  Loading CSV file: {path}")
        df = pd.read_csv(path)
        print(f"    -> {len(df)} rows")
    
    return df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names: strip whitespace."""
    df.columns = df.columns.str.strip()
    return df


def clean_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert columns to appropriate types."""
    
    # Numeric columns
    num_cols = ['price', 'm2 construidos', 'm2 utiles', 'habs', 'banos', 'construido en']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace('.', '', regex=False)
                       .str.replace(',', '.', regex=False)
                       .str.replace(' ', '', regex=False),
                errors='coerce'
            )
    
    # Boolean columns
    bool_cols = ['Terraza', 'Garaje', 'ascensor', 'piscina', 'Trastero', 'Armarios']
    for col in bool_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().isin(['si', 'sí', 'true', '1', 'yes'])
    
    return df


def main():
    parser = argparse.ArgumentParser(
        description='Train rent estimation ML model',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python train_model.py --data alquiler.xlsx
  python train_model.py --data alquiler.xlsx --outdir models/ --with_quantiles
  python train_model.py --data data.csv --target price --cv_splits 5
        """
    )
    
    parser.add_argument('--data', required=True,
                        help='Path to training data (Excel or CSV)')
    parser.add_argument('--target', default='price',
                        help='Target column name (default: price)')
    parser.add_argument('--outdir', default='.',
                        help='Output directory for model files')
    parser.add_argument('--cv_splits', type=int, default=5,
                        help='Number of cross-validation splits')
    parser.add_argument('--with_quantiles', action='store_true',
                        help='Train quantile models for confidence intervals')
    parser.add_argument('--use_text', action='store_true',
                        help='Use TF-IDF on Titulo/Descripcion (slower)')
    parser.add_argument('--round_to', type=int, default=25,
                        help='Round predictions to this value (default: 25)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("TRAIN RENT ESTIMATION MODEL")
    print("=" * 60)
    print(f"  Data file: {args.data}")
    print(f"  Target: {args.target}")
    print(f"  Output dir: {args.outdir}")
    print(f"  Round to: {args.round_to}€")
    print()
    
    # Create output directory
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    
    # Load data
    print("STEP 1: Loading data...")
    df = load_table(args.data)
    df = normalize_columns(df)
    df = clean_types(df)
    
    # Check target exists
    if args.target not in df.columns:
        print(f"ERROR: Target column '{args.target}' not found!")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    # Remove leakage columns
    print("\nSTEP 2: Removing leakage columns...")
    for col in LEAKAGE_COLUMNS:
        if col in df.columns and col != args.target:
            print(f"  Dropping: {col}")
            df = df.drop(columns=[col])
    
    # Train model
    print("\nSTEP 3: Training model...")
    try:
        model_dict = train_rent_model(
            df, 
            target_col=args.target,
            round_to=args.round_to
        )
    except Exception as e:
        print(f"ERROR during training: {e}")
        sys.exit(1)
    
    # Save model
    print("\nSTEP 4: Saving model...")
    model_path = outdir / 'rent_model.joblib'
    joblib.dump(model_dict, model_path)
    print(f"  Saved: {model_path}")
    
    # Save metadata
    metadata = {
        'trained_at': datetime.now().isoformat(),
        'data_file': str(args.data),
        'target_column': args.target,
        'n_samples': model_dict['metrics']['n_samples'],
        'n_features': model_dict['metrics']['n_features'],
        'metrics': {
            'mae': round(model_dict['metrics']['mae'], 2),
            'mae_std': round(model_dict['metrics']['mae_std'], 2),
            'r2': round(model_dict['metrics']['r2'], 4),
        },
        'feature_names': model_dict['feature_names'],
        'round_to': args.round_to
    }
    
    metadata_path = outdir / 'metadata.json'
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    print(f"  Saved: {metadata_path}")
    
    # Summary
    print("\n" + "=" * 60)
    print("TRAINING COMPLETE")
    print("=" * 60)
    print(f"  Model: {model_path}")
    print(f"  MAE: {metadata['metrics']['mae']:.2f}€ (±{metadata['metrics']['mae_std']:.2f})")
    print(f"  R²: {metadata['metrics']['r2']:.4f}")
    print(f"  Samples: {metadata['n_samples']}")
    print()
    print("To predict, run:")
    print(f"  python predict_model.py --model {model_path} --input venta.xlsx --output predictions.csv")
    print()


if __name__ == '__main__':
    main()
