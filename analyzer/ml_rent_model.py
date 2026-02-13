"""
=============================================================================
ML RENT MODEL - Módulo de Estimación de Precio de Alquiler
=============================================================================
Modelo hedónico + comparables para estimar renta mensual (€/mes).

Incluye:
- Feature engineering avanzado
- Ensemble: Ridge + KNN + HistGradientBoosting
- Cuantiles p5/p95 para intervalos de confianza
- Cálculo de precisión por predicción
=============================================================================
"""

import numpy as np
import pandas as pd
from datetime import datetime
from typing import Tuple, Dict, Optional

# =============================================================================
# SUBTASK 1.1: FEATURE ENGINEERING FUNCTIONS
# =============================================================================

def parse_altura(altura_str: str) -> Tuple[Optional[int], bool, bool]:
    """
    Parse altura string to numeric value and flags.
    
    Returns: (altura_num, es_bajo, es_atico)
    """
    if pd.isna(altura_str):
        return None, False, False
    
    altura_str = str(altura_str).lower().strip()
    
    # Check for ático/atico
    if 'ático' in altura_str or 'atico' in altura_str:
        return 99, False, True
    
    # Check for bajo/entresuelo
    if 'bajo' in altura_str or 'entresuelo' in altura_str:
        return 0, True, False
    
    # Extract numeric value (e.g., "3ª", "2º", "planta 4")
    import re
    match = re.search(r'(\d+)', altura_str)
    if match:
        num = int(match.group(1))
        return num, num == 0, False
    
    return None, False, False


def create_grid_id(lat: float, lon: float, precision: int = 3) -> str:
    """
    Create grid identifier from lat/lon coordinates.
    
    Args:
        lat: Latitude
        lon: Longitude  
        precision: Decimal places for rounding (default 3 = ~100m grid)
    
    Returns: Grid ID string like "40.415_-3.704"
    """
    if pd.isna(lat) or pd.isna(lon):
        return "unknown"
    
    lat_round = round(float(lat), precision)
    lon_round = round(float(lon), precision)
    return f"{lat_round}_{lon_round}"


def calculate_stock_by_grid(df: pd.DataFrame, grid_col: str = 'grid_id') -> pd.Series:
    """
    Calculate property density (stock) per grid zone.
    
    Returns: Series with stock count per grid_id
    """
    return df.groupby(grid_col)[grid_col].transform('count')


def prepare_features(df: pd.DataFrame, current_year: int = None) -> pd.DataFrame:
    """
    Create derived features for ML model.
    
    Features created:
    - log_m2: log1p(m2 construidos)
    - m2_por_hab: m2 construidos / habitaciones
    - banos_por_hab: baños / habitaciones
    - edad: años desde construcción
    - grid_id: micro-zona desde lat/lon
    - stock_grid: densidad de propiedades en la zona
    - es_bajo: flag planta baja
    - es_atico: flag ático
    - altura_num: altura numérica
    - mes_scraping: mes del scraping (estacionalidad)
    
    Args:
        df: DataFrame with raw property data
        current_year: Year for age calculation (default: current year)
    
    Returns: DataFrame with additional features
    """
    if current_year is None:
        current_year = datetime.now().year
    
    df = df.copy()
    
    # --- Numeric transformations ---
    
    # Log transform of m2 (elasticity)
    m2_col = 'm2 construidos' if 'm2 construidos' in df.columns else 'm2_construidos'
    if m2_col in df.columns:
        df['log_m2'] = np.log1p(df[m2_col].fillna(0))
    
    # m2 per bedroom
    habs_col = 'habs' if 'habs' in df.columns else 'habitaciones'
    if m2_col in df.columns and habs_col in df.columns:
        habs_safe = df[habs_col].fillna(1).replace(0, 1)
        df['m2_por_hab'] = df[m2_col] / habs_safe
    
    # Bathrooms per bedroom
    banos_col = 'banos' if 'banos' in df.columns else 'baños'
    if banos_col in df.columns and habs_col in df.columns:
        df['banos_por_hab'] = df[banos_col].fillna(1) / habs_safe
    
    # --- Age calculation ---
    year_col = 'construido en' if 'construido en' in df.columns else 'año_construccion'
    if year_col in df.columns:
        df['edad'] = current_year - pd.to_numeric(df[year_col], errors='coerce')
        df['edad'] = df['edad'].clip(lower=0, upper=150)  # Sanity check
    
    # --- Altura parsing ---
    altura_col = 'altura' if 'altura' in df.columns else None
    if altura_col:
        parsed = df[altura_col].apply(parse_altura)
        df['altura_num'] = parsed.apply(lambda x: x[0])
        df['es_bajo'] = parsed.apply(lambda x: x[1])
        df['es_atico'] = parsed.apply(lambda x: x[2])
    else:
        df['altura_num'] = None
        df['es_bajo'] = False
        df['es_atico'] = False
    
    # --- Grid ID from coordinates ---
    lat_col = 'Lat' if 'Lat' in df.columns else 'lat'
    lon_col = 'Lon' if 'Lon' in df.columns else 'lon'
    if lat_col in df.columns and lon_col in df.columns:
        df['grid_id'] = df.apply(
            lambda row: create_grid_id(row.get(lat_col), row.get(lon_col)), 
            axis=1
        )
        df['stock_grid'] = calculate_stock_by_grid(df, 'grid_id')
    else:
        df['grid_id'] = 'unknown'
        df['stock_grid'] = 1
    
    # --- Seasonality from scraping date ---
    fecha_col = 'Fecha Scraping' if 'Fecha Scraping' in df.columns else 'fecha_scraping'
    if fecha_col in df.columns:
        try:
            df['fecha_parsed'] = pd.to_datetime(df[fecha_col], errors='coerce')
            df['mes_scraping'] = df['fecha_parsed'].dt.month
            df['año_scraping'] = df['fecha_parsed'].dt.year
            df = df.drop(columns=['fecha_parsed'])
        except:
            df['mes_scraping'] = 1
            df['año_scraping'] = current_year
    else:
        df['mes_scraping'] = 1
        df['año_scraping'] = current_year
    
    # --- Ensure Binary Features are Numeric (0/1) ---
    # This prevents SimpleImputer errors with boolean input
    for col in ['ascensor', 'Garaje', 'Terraza', 'piscina', 'es_bajo', 'es_atico']:
        if col in df.columns:
            # Handle boolean types, strings 'si'/'no', or existing 0/1
            # If it's already boolean or object, convert to int
            df[col] = df[col].replace({'si': 1, 'Si': 1, 'sí': 1, 'Sí': 1, 
                                      'yes': 1, 'true': 1, 'True': 1,
                                      'no': 0, 'No': 0, 'false': 0, 'False': 0})
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
    return df


# =============================================================================
# FEATURE COLUMNS DEFINITION
# =============================================================================

# Columns to use in the model (will be populated by build_preprocessor)
NUMERIC_FEATURES = [
    'log_m2', 'm2_por_hab', 'banos_por_hab', 'edad', 
    'altura_num', 'stock_grid', 'mes_scraping'
]

CATEGORICAL_FEATURES = [
    'Distrito', 'tipo', 'estado', 'orientacion'
]

BINARY_FEATURES = [
    'ascensor', 'Garaje', 'Terraza', 'piscina', 'es_bajo', 'es_atico'
]

# Columns that would leak target information (exclude from features)
LEAKAGE_COLUMNS = [
    'price', 'old price', 'price change %', 'precio por m2', 
    'precio_m2', 'renta_estimada'
]


# =============================================================================
# SUBTASK 1.2: PREPROCESSOR PIPELINE
# =============================================================================

def build_preprocessor(df: pd.DataFrame) -> Tuple['ColumnTransformer', list]:
    """
    Build sklearn ColumnTransformer for preprocessing features.
    
    Handles:
    - Numeric: SimpleImputer(median) + StandardScaler
    - Categorical: SimpleImputer(most_frequent) + OneHotEncoder
    - Binary: SimpleImputer(0) + passthrough
    
    Args:
        df: DataFrame to inspect for available columns
        
    Returns:
        (preprocessor, feature_names): ColumnTransformer and list of feature names used
    """
    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import StandardScaler, OneHotEncoder
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    
    # Filter to columns that actually exist in the dataframe
    available_numeric = [col for col in NUMERIC_FEATURES if col in df.columns]
    available_categorical = [col for col in CATEGORICAL_FEATURES if col in df.columns]
    available_binary = [col for col in BINARY_FEATURES if col in df.columns]
    
    # Numeric pipeline: impute median + scale
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])
    
    # Categorical pipeline: impute most frequent + one-hot encode
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
    ])
    
    # Binary pipeline: impute with 0 (False) + passthrough as float
    binary_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value=0)),
    ])
    
    # Build ColumnTransformer
    transformers = []
    
    if available_numeric:
        transformers.append(('num', numeric_transformer, available_numeric))
    
    if available_categorical:
        transformers.append(('cat', categorical_transformer, available_categorical))
    
    if available_binary:
        transformers.append(('bin', binary_transformer, available_binary))
    
    preprocessor = ColumnTransformer(
        transformers=transformers,
        remainder='drop',  # Drop columns not specified
        verbose_feature_names_out=True
    )
    
    feature_names = available_numeric + available_categorical + available_binary
    
    return preprocessor, feature_names


def get_available_features(df: pd.DataFrame) -> Dict[str, list]:
    """
    Get lists of available features by type from a DataFrame.
    
    Returns dict with keys: 'numeric', 'categorical', 'binary', 'all'
    """
    available = {
        'numeric': [col for col in NUMERIC_FEATURES if col in df.columns],
        'categorical': [col for col in CATEGORICAL_FEATURES if col in df.columns],
        'binary': [col for col in BINARY_FEATURES if col in df.columns],
    }
    available['all'] = available['numeric'] + available['categorical'] + available['binary']
    return available


# =============================================================================
# SUBTASK 1.3: MODEL TRAINING FUNCTION
# =============================================================================

def train_rent_model(df_alquiler: pd.DataFrame, 
                     target_col: str = 'price',
                     round_to: int = 25) -> Dict:
    """
    Train ML models for rent estimation.
    
    Models included:
    - Ridge: Linear model on log(price), interpretable
    - KNN: K-Nearest Neighbors for comparable-based estimation
    - HGB: HistGradientBoosting for non-linear patterns
    - Ensemble: VotingRegressor combining above models
    - Quantile models: p5/p95 for confidence intervals
    
    Args:
        df_alquiler: DataFrame with rental listings (training data)
        target_col: Column name for target variable (rent price)
        round_to: Round predictions to this value (default 25€)
        
    Returns:
        Dict with keys: 'ensemble', 'q05', 'q95', 'preprocessor', 
                       'feature_names', 'metrics', 'round_to'
    """
    from sklearn.linear_model import Ridge
    from sklearn.neighbors import KNeighborsRegressor
    from sklearn.ensemble import HistGradientBoostingRegressor, VotingRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.model_selection import cross_val_score
    import warnings
    
    print("  [ML] Training rent estimation model...")
    
    # Prepare features
    df = prepare_features(df_alquiler.copy())
    
    # Remove rows with missing target
    df = df.dropna(subset=[target_col])
    
    if len(df) < 10:
        raise ValueError(f"Not enough data to train model: {len(df)} rows")
    
    # Get target (use log transform for Ridge)
    y = df[target_col].values
    y_log = np.log1p(y)
    
    # Build preprocessor
    preprocessor, feature_names = build_preprocessor(df)
    
    # Fit preprocessor and transform
    X = preprocessor.fit_transform(df)
    
    print(f"    -> Training data: {X.shape[0]} samples, {X.shape[1]} features")
    
    # --- Individual Models ---
    
    # Ridge on log(price)
    ridge = Ridge(alpha=1.0)
    
    # KNN for comparable-style estimation
    n_neighbors = min(10, len(df) // 5, 50)  # Adaptive K
    n_neighbors = max(3, n_neighbors)  # At least 3
    knn = KNeighborsRegressor(
        n_neighbors=n_neighbors,
        weights='distance',
        metric='euclidean'
    )
    
    # HistGradientBoosting for non-linear
    hgb = HistGradientBoostingRegressor(
        max_iter=100,
        max_depth=8,
        learning_rate=0.1,
        random_state=42,
        early_stopping=False
    )
    
    # --- Train individual models ---
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        
        # Ridge on log scale
        ridge.fit(X, y_log)
        
        # KNN on original scale
        knn.fit(X, y)
        
        # HGB on original scale
        hgb.fit(X, y)
    
    # --- Ensemble (VotingRegressor) ---
    # We create a custom ensemble that combines predictions
    # Note: Ridge predicts log, need to exp() before averaging
    
    # Create fresh models for ensemble
    ensemble = VotingRegressor(
        estimators=[
            ('knn', KNeighborsRegressor(n_neighbors=n_neighbors, weights='distance')),
            ('hgb', HistGradientBoostingRegressor(max_iter=100, max_depth=8, 
                                                   learning_rate=0.1, random_state=42))
        ],
        weights=[0.3, 0.7]  # Weight HGB more (usually better)
    )
    ensemble.fit(X, y)
    
    # --- Quantile Models for Confidence Intervals ---
    print("    -> Training quantile models (p5/p95)...")
    
    q05_model = HistGradientBoostingRegressor(
        loss='quantile',
        quantile=0.05,
        max_iter=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42
    )
    
    q95_model = HistGradientBoostingRegressor(
        loss='quantile',
        quantile=0.95,
        max_iter=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42
    )
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        q05_model.fit(X, y)
        q95_model.fit(X, y)
    
    # --- Cross-validation metrics ---
    print("    -> Calculating cross-validation metrics...")
    
    cv_scores = cross_val_score(ensemble, X, y, cv=min(5, len(df)//10), 
                                 scoring='neg_mean_absolute_error')
    mae = -cv_scores.mean()
    mae_std = cv_scores.std()
    
    r2_scores = cross_val_score(ensemble, X, y, cv=min(5, len(df)//10), 
                                 scoring='r2')
    r2 = r2_scores.mean()
    
    metrics = {
        'mae': mae,
        'mae_std': mae_std,
        'r2': r2,
        'n_samples': len(df),
        'n_features': X.shape[1],
        'n_neighbors_knn': n_neighbors
    }
    
    print(f"    -> MAE: {mae:.2f}€ (±{mae_std:.2f})")
    print(f"    -> R²: {r2:.3f}")
    
    # --- Return model dictionary ---
    model_dict = {
        'ensemble': ensemble,
        'ridge': ridge,  # Keep for interpretability
        'q05': q05_model,
        'q95': q95_model,
        'preprocessor': preprocessor,
        'feature_names': feature_names,
        'metrics': metrics,
        'round_to': round_to,
        'y_log_mean': y_log.mean(),  # For Ridge predictions
        'y_log_std': y_log.std()
    }
    
    print("  [ML] Model training complete!")
    
    return model_dict


# =============================================================================
# SUBTASK 1.4: PREDICTION AND PRECISION FUNCTIONS
# =============================================================================

def round_to_nearest(value: float, base: int = 25) -> int:
    """Round value to nearest base (default 25€)."""
    return int(base * round(value / base))


def predict_rent(model_dict: Dict, 
                 df_venta: pd.DataFrame,
                 include_range: bool = True) -> pd.DataFrame:
    """
    Predict rent prices for properties using trained model.
    
    Args:
        model_dict: Dictionary from train_rent_model()
        df_venta: DataFrame with properties to predict
        include_range: Whether to include p5/p95 confidence interval
        
    Returns:
        DataFrame with added columns:
        - renta_estimada: Point estimate (rounded)
        - renta_p05: Lower bound (5th percentile)
        - renta_p95: Upper bound (95th percentile)  
        - renta_rango: String format "850€ - 950€"
    """
    print("  [ML] Predicting rent prices...")
    
    # Prepare features
    df = prepare_features(df_venta.copy())
    
    # Get preprocessor
    preprocessor = model_dict['preprocessor']
    round_to = model_dict.get('round_to', 25)
    
    # Transform features
    try:
        X = preprocessor.transform(df)
    except Exception as e:
        print(f"    [WARN] Preprocessor transform failed: {e}")
        # Fallback: return NaN predictions
        df['renta_estimada'] = np.nan
        df['renta_p05'] = np.nan
        df['renta_p95'] = np.nan
        df['renta_rango'] = "N/A"
        return df
    
    # Predict with ensemble
    ensemble = model_dict['ensemble']
    predictions = ensemble.predict(X)
    
    # Round predictions
    df['renta_estimada'] = [round_to_nearest(p, round_to) for p in predictions]
    
    # Predict quantiles for confidence interval
    if include_range and 'q05' in model_dict and 'q95' in model_dict:
        q05_preds = model_dict['q05'].predict(X)
        q95_preds = model_dict['q95'].predict(X)
        
        df['renta_p05'] = [round_to_nearest(p, round_to) for p in q05_preds]
        df['renta_p95'] = [round_to_nearest(p, round_to) for p in q95_preds]
        
        # Create range string
        df['renta_rango'] = df.apply(
            lambda row: f"{int(row['renta_p05'])}€ - {int(row['renta_p95'])}€",
            axis=1
        )
    else:
        # Estimate range as ±10% if quantile models not available
        df['renta_p05'] = (df['renta_estimada'] * 0.9).astype(int)
        df['renta_p95'] = (df['renta_estimada'] * 1.1).astype(int)
        df['renta_rango'] = df.apply(
            lambda row: f"{int(row['renta_p05'])}€ - {int(row['renta_p95'])}€",
            axis=1
        )
    
    print(f"    -> Predicted {len(df)} properties")
    print(f"    -> Mean rent: {df['renta_estimada'].mean():.0f}€")
    print(f"    -> Range: {df['renta_estimada'].min():.0f}€ - {df['renta_estimada'].max():.0f}€")
    
    return df


# =============================================================================
# SUBTASK 1.5: HEDONIC ADJUSTMENTS & HIERARCHICAL WEIGHTS
# =============================================================================

# Hedonic Adjustment Coefficients (Approximated for Spanish market)
HEDONIC_COEFFS = {
    'hab': 150.0,      # €/month per bedroom
    'bano': 75.0,      # €/month per bathroom
    'garaje': 80.0,    # €/month for garage (flat value)
    'reforma': 0.15,   # +15% for renovated vs good condition
    'obra_nueva': 0.25,# +25% for new build vs good condition
    'atico': 0.15,     # +15% for penthouse
    'bajo': -0.10,     # -10% for ground floor (without garden)
    'exterior': 0.10,  # +10% for exterior vs interior
    'piscina': 0.10,   # +10% for pool/common areas
    'depreciation_year': 0.005 # 0.5% per year
}

def apply_hedonic_adjustment(comp_price: float, comp: pd.Series, target: pd.Series) -> float:
    """
    Adjust a comparable's price to match the target property's features.
    
    Logic: If Target is BETTER than Comp, we ADJUST Comp price UP.
           If Target is WORSE than Comp, we ADJUST Comp price DOWN.
    """
    adj_price = float(comp_price)
    
    # 1. Rooms & Bathrooms (Flat adjustments)
    t_habs = target.get('habs', 2) or 2
    c_habs = comp.get('habs', 2) or 2
    adj_price += (t_habs - c_habs) * HEDONIC_COEFFS['hab']
    
    t_banos = target.get('banos', 1) or 1
    c_banos = comp.get('banos', 1) or 1
    adj_price += (t_banos - c_banos) * HEDONIC_COEFFS['bano']
    
    # 2. M2 adjustment (Proportional to price/m2)
    m2_col = 'm2 construidos' if 'm2 construidos' in target.index else 'm2_construidos'
    t_m2 = target.get(m2_col, 80) or 80
    c_m2 = comp.get(m2_col, 80) or 80
    if c_m2 > 0:
        price_m2 = comp_price / c_m2
        adj_price += (t_m2 - c_m2) * price_m2 * 0.7 # 70% efficiency on extra m2
        
    # 3. Extras (Garaje, Ascensor, Terraza)
    t_garaje = bool(target.get('Garaje') or target.get('garaje'))
    c_garaje = bool(comp.get('Garaje') or comp.get('garaje'))
    if t_garaje and not c_garaje: adj_price += HEDONIC_COEFFS['garaje']
    elif not t_garaje and c_garaje: adj_price -= HEDONIC_COEFFS['garaje']
    
    # 4. Building Features (Percentage adjustments)
    mult = 1.0
    
    # Condition (Estado)
    t_estado = str(target.get('estado', '')).lower()
    c_estado = str(comp.get('estado', '')).lower()
    if 'nueva' in t_estado and 'nueva' not in c_estado: mult += HEDONIC_COEFFS['obra_nueva']
    elif 'reformar' in t_estado and 'reformar' not in c_estado: mult -= HEDONIC_COEFFS['reforma']
    
    # Height (Ático/Bajo)
    t_alt = str(target.get('altura', '')).lower()
    c_alt = str(comp.get('altura', '')).lower()
    if 'ático' in t_alt and 'ático' not in c_alt: mult += HEDONIC_COEFFS['atico']
    elif 'bajo' in t_alt and 'bajo' not in c_alt: mult += HEDONIC_COEFFS['bajo']
    
    # Piscina
    t_piscina = bool(target.get('piscina'))
    c_piscina = bool(comp.get('piscina'))
    if t_piscina and not c_piscina: mult += HEDONIC_COEFFS['piscina']
    
    adj_price *= mult
    
    return max(0, adj_price)

def calculate_precision_score(venta_row: pd.Series, 
                               comparables_df: pd.DataFrame,
                               weights: Dict = None) -> float:
    """
    Calculate precision score (0-100%) based on Two-Plane Similarity:
    Plane 1: Geometric Hierarchy (Barrio > Distrito > Ciudad) - WEIGHT MULTIPLIER
    Plane 2: Physical Similarity (m2, habs, banos, extras) - SCORE BASE
    """
    if comparables_df is None or len(comparables_df) == 0:
        return 0.0
    
    # 1. Location Plane (Hierarchy)
    v_barrio = str(venta_row.get('Barrio', '')).lower()
    v_distrito = str(venta_row.get('Distrito', '')).lower()
    v_ciudad = str(venta_row.get('Ciudad', '')).lower()
    
    # Prepare Physical Weights (Plane 2)
    if weights is None:
        # Refined physical weights (relative within plane 2)
        weights = {
            'm2': 0.30, 
            'habs': 0.25, 
            'banos': 0.20, 
            'type': 0.15,
            'extras': 0.10
        }
    
    def norm_tipo(t):
        t = str(t).lower()
        if 'casa' in t or 'chalet' in t: return 'casa'
        return 'piso'

    v_tipo = norm_tipo(venta_row.get('tipo', 'piso'))
    m2_col = 'm2 construidos' if 'm2 construidos' in venta_row.index else 'm2_construidos'
    v_m2 = venta_row.get(m2_col, 80) or 80
    v_habs = venta_row.get('habs', 2) or 2
    v_banos = venta_row.get('banos', 1) or 1
    
    weighted_scores = []
    
    for _, comp in comparables_df.iterrows():
        # --- Plane 1: Geometric Weight ---
        c_barrio = str(comp.get('Barrio', '')).lower()
        c_distrito = str(comp.get('Distrito', '')).lower()
        c_ciudad = str(comp.get('Ciudad', '')).lower()
        
        loc_weight = 0.2 # Default: Same City
        if v_barrio == c_barrio and v_barrio != '':
            loc_weight = 1.0 # Same Barrio
        elif v_distrito == c_distrito and v_distrito != '':
            loc_weight = 0.6 # Same Distrito
        
        # --- Plane 2: Physical Similarity ---
        phys_score = 0.0
        
        # M2 (30%)
        c_m2 = comp.get(m2_col, 80) or 80
        m2_sim = max(0, 1 - abs(c_m2 - v_m2) / v_m2)
        phys_score += weights['m2'] * m2_sim
        
        # Habs (25%)
        c_habs = comp.get('habs', 2) or 2
        phys_score += weights['habs'] * (1.0 if c_habs == v_habs else (0.5 if abs(c_habs - v_habs) == 1 else 0))
        
        # Banos (20%)
        c_banos = comp.get('banos', 1) or 1
        phys_score += weights['banos'] * (1.0 if c_banos == v_banos else (0.5 if abs(c_banos - v_banos) == 1 else 0))
        
        # Type (15%)
        c_tipo = norm_tipo(comp.get('tipo', 'piso'))
        phys_score += weights['type'] * (1.0 if c_tipo == v_tipo else 0)
        
        # Extras (10%)
        # Simple match count
        extras_match = 0
        for extra in ['Garaje', 'Terraza', 'ascensor']:
            v_e = bool(venta_row.get(extra) or venta_row.get(extra.lower()))
            c_e = bool(comp.get(extra) or comp.get(extra.lower()))
            if v_e == c_e: extras_match += 1
        phys_score += weights['extras'] * (extras_match / 3.0)
        
        # Combine Plane 1 and Plane 2
        # Use Product for "Combined match quality"
        final_weight = loc_weight * (phys_score ** 1.5) # Squaring improves priority of physically similar items
        weighted_scores.append(final_weight)
        
    # Scale final result (bonus for volume of matches)
    base_avg = np.mean(weighted_scores) if weighted_scores else 0
    n_bonus = min(0.3, len(comparables_df) * 0.05) # Max +30% for volume
    
    precision = min(100.0, (base_avg + n_bonus) * 100)
    return round(precision, 1)


def calculate_precision_for_prediction(venta_row: pd.Series,
                                       df_alquiler: pd.DataFrame,
                                       comparables: list = None) -> float:
    """
    Calculate precision score for a single prediction.
    
    If comparables list is provided, uses those.
    Otherwise, finds comparables from df_alquiler.
    
    Returns:
        Precision score 0-100%
    """
    if comparables and len(comparables) > 0:
        # Convert list of dicts to DataFrame
        if isinstance(comparables[0], dict):
            comparables_df = pd.DataFrame(comparables)
        else:
            comparables_df = comparables
        return calculate_precision_score(venta_row, comparables_df)
    
    # Find comparables if not provided
    # (Simple version - real implementation in analysis.py)
    distrito = venta_row.get('Distrito')
    if distrito is None or df_alquiler is None:
        return 0.0
    
    mask = df_alquiler['Distrito'] == distrito
    if mask.sum() == 0:
        return 0.0
    
    return calculate_precision_score(venta_row, df_alquiler[mask].head(10))
