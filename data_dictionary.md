# Data Dictionary: API vs Scraper

| Field | API JSON Path | Scraper Column | Notes |
| :--- | :--- | :--- | :--- |
| **ID** | `propertyCode` | *Generated/Inferred* | API gives explicit ID. Scraper relies on URL hash. |
| **URL** | `url` | `URL` | Match. |
| **Title** | `suggestedTexts.title` | `Título` | API title is cleaner. |
| **Price** | `price` | `Precio` | Match. |
| **Price Drop** | `priceInfo.priceDropInfo.priceDropValue` | *Calculated* | API gives explicit drop value & date (`dropDate`). Scraper only sees current. |
| **Type** | `detailedType.typology` / `propertyType` | `tipo` | API: "flat", "chalet", "studio". Scraper: Inferred from title ("Piso", "Chalet"). |
| **Size** | `size` | `m2 construidos` | Match. |
| **Rooms** | `rooms` | `habs` | Match. |
| **Bathrooms** | `bathrooms` | `banos` | Match. |
| **Floor** | `floor` | `planta` | API: "4", "bj". Scraper: "4ª", "Bajo". Needs normalization. |
| **District** | `district` | `Distrito` | API is robust. Scraper parses breadcrumbs. |
| **Neighborhood** | `neighborhood` | `Barrio` | API is robust. Scraper parses breadcrumbs. |
| **Garage** | `features.hasParking` (Assumed) or `parkingSpace` | `garaje` | Need to verify API parking field. |
| **Terrace** | `features.hasTerrace` | `terraza` | API boolean. Scraper boolean. |
| **Lift** | `hasLift` | `ascensor` | API boolean. Scraper boolean. |
| **Pool** | `features.hasSwimmingPool` | `piscina` | API boolean. |
| **AC** | `features.hasAirConditioning` | `aire acondicionado` | API boolean. |
| **Store Room** | `features.hasBoxRoom` | `trastero` | API boolean. |
| **Garden** | `features.hasGarden` | `jardín` | API boolean. |
| **Condition** | `status` | `estado` | API: "good", "renew". Scraper: "Buen estado". |
| **Agency** | `contactInfo.commercialName` | *Not collected* | API gives agency name (valuable). |
| **Coordinates** | `latitude`, `longitude` | *Not collected* | API gives exact(ish) coords. Scraper maps only provide approx presence. |
