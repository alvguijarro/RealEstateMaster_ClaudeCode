#!/usr/bin/env python
"""
=============================================================================
PREDICT MODEL - Standalone CLI for Rent Prediction
=============================================================================
Predict rental prices for properties using a trained model.

Usage:
    python predict_model.py --model rent_model.joblib --input venta.xlsx --output predictions.csv
    python predict_model.py --model rent_model.joblib --input venta.xlsx --round_to 25

Output:
    - predictions.csv - CSV with predictions and confidence intervals
=============================================================================
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

import pandas as pd
import numpy as np
import joblib

from ml_rent_model import prepare_features, round_to_nearest


def load_table(path: str) -> pd.DataFrame:
    """Load data from Excel (multi-sheet) or CSV file."""
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
        print(f"  Total: {len(df)} rows")
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
        description='Predict rent prices using trained model',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python predict_model.py --model rent_model.joblib --input venta.xlsx
  python predict_model.py --model rent_model.joblib --input venta.xlsx --output predictions.csv
  python predict_model.py --model models/rent_model.joblib --input data.csv --round_to 50
        """
    )
    
    parser.add_argument('--model', required=True,
                        help='Path to trained model (.joblib)')
    parser.add_argument('--input', required=True,
                        help='Path to input data (Excel or CSV)')
    parser.add_argument('--output', default=None,
                        help='Output CSV path (default: predictions_YYYYMMDD.csv)')
    parser.add_argument('--round_to', type=int, default=None,
                        help='Override rounding (default: use model setting)')
    parser.add_argument('--include_yields', action='store_true',
                        help='Calculate gross yield (requires price column)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("PREDICT RENT PRICES")
    print("=" * 60)
    print(f"  Model: {args.model}")
    print(f"  Input: {args.input}")
    print()
    
    # Load model
    print("STEP 1: Loading model...")
    model_path = Path(args.model)
    if not model_path.exists():
        print(f"ERROR: Model file not found: {model_path}")
        sys.exit(1)
    
    model_dict = joblib.load(model_path)
    print(f"  Loaded model with {model_dict['metrics']['n_features']} features")
    print(f"  Model MAE: {model_dict['metrics']['mae']:.2f}€")
    
    # Get rounding setting
    round_to = args.round_to or model_dict.get('round_to', 25)
    print(f"  Rounding to: {round_to}€")
    
    # Load input data
    print("\nSTEP 2: Loading input data...")
    df = load_table(args.input)
    df = normalize_columns(df)
    df = clean_types(df)
    
    # Prepare features
    print("\nSTEP 3: Preparing features...")
    df = prepare_features(df)
    
    # Get preprocessor
    preprocessor = model_dict['preprocessor']
    
    # Transform features
    print("\nSTEP 4: Predicting...")
    try:
        X = preprocessor.transform(df)
    except Exception as e:
        print(f"ERROR: Failed to transform features: {e}")
        print("This may happen if the input data has different columns than training data.")
        sys.exit(1)
    
    # Predict with ensemble
    ensemble = model_dict['ensemble']
    predictions = ensemble.predict(X)
    
    # Round predictions
    df['renta_estimada'] = [round_to_nearest(p, round_to) for p in predictions]
    
    # Predict quantiles for confidence interval
    if 'q05' in model_dict and 'q95' in model_dict:
        q05_preds = model_dict['q05'].predict(X)
        q95_preds = model_dict['q95'].predict(X)
        
        df['renta_p05'] = [round_to_nearest(p, round_to) for p in q05_preds]
        df['renta_p95'] = [round_to_nearest(p, round_to) for p in q95_preds]
        df['renta_rango'] = df.apply(
            lambda row: f"{int(row['renta_p05'])}€ - {int(row['renta_p95'])}€",
            axis=1
        )
    else:
        # Estimate range as ±15%
        df['renta_p05'] = (df['renta_estimada'] * 0.85).astype(int)
        df['renta_p95'] = (df['renta_estimada'] * 1.15).astype(int)
        df['renta_rango'] = df.apply(
            lambda row: f"{int(row['renta_p05'])}€ - {int(row['renta_p95'])}€",
            axis=1
        )
    
    # Calculate yields if requested
    if args.include_yields and 'price' in df.columns:
        print("  Calculating yields...")
        df['yield_bruta'] = (12 * df['renta_estimada']) / df['price']
        df['yield_bruta_%'] = (df['yield_bruta'] * 100).round(2)
        df['años_recuperacion'] = (df['price'] / (12 * df['renta_estimada'])).round(1)
    
    # Select output columns
    output_cols = ['renta_estimada', 'renta_p05', 'renta_p95', 'renta_rango']
    
    # Add identifiers if available
    for col in ['URL', 'Titulo', 'titulo', 'Distrito', 'm2 construidos', 'habs', 'banos', 'price']:
        if col in df.columns:
            output_cols.insert(0, col)
    
    # Add yields if calculated
    if 'yield_bruta_%' in df.columns:
        output_cols.extend(['yield_bruta_%', 'años_recuperacion'])
    
    # Remove duplicates while preserving order
    output_cols = list(dict.fromkeys(output_cols))
    
    # Filter to existing columns
    output_cols = [c for c in output_cols if c in df.columns]
    
    df_output = df[output_cols]
    
    # Save output
    print("\nSTEP 5: Saving results...")
    if args.output:
        output_path = Path(args.output)
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = Path(f'predictions_{timestamp}.csv')
    
    df_output.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"  Saved: {output_path}")
    
    # Summary
    print("\n" + "=" * 60)
    print("PREDICTION COMPLETE")
    print("=" * 60)
    print(f"  Properties: {len(df)}")
    print(f"  Mean rent: {df['renta_estimada'].mean():.0f}€/mes")
    print(f"  Range: {df['renta_estimada'].min():.0f}€ - {df['renta_estimada'].max():.0f}€")
    if 'yield_bruta_%' in df.columns:
        print(f"  Mean yield: {df['yield_bruta_%'].mean():.2f}%")
    print(f"\n  Output: {output_path}")
    print()


if __name__ == '__main__':
    main()
