# OpenDataHub
Projet de TER de dernière année de Master SID
TER OPENDATAHUB !

# Architecture Globale du projet proposer 


data_pipeline/
├── README.md # Documentation du projet
├── requirements.txt # Dépendances Python
├── .env # Variables d'environnement (DB credentials)
├── .gitignore # Fichiers à ignorer
├── main.py # Point d'entrée principal
├── pipeline.py # Orchestrateur du pipeline ETL
├── config/ # Configuration globale
│ ├── init.py
│ ├── settings.py # Configuration globale
│ ├── schemas.py # Définition des schémas de données
│ └── database.py # Connexion aux bases de données
├── extractors/ # Extraction (Extract)
│ ├── init.py
│ ├── base_extractor.py # Classe abstraite de base
│ ├── csv_extractor.py # Extracteur CSV
│ ├── json_extractor.py # Extracteur JSON
│ ├── excel_extractor.py # Extracteur Excel (XLSX, XLS)
│ ├── xml_extractor.py # Extracteur XML
│ └── api_extractor.py # Extracteur API REST (bonus)
├── transformers/ # Transformation (Transform)
│ ├── init.py
│ ├── base_transformer.py # Classe de transformation de base
│ ├── cleaner.py # Nettoyage des données
│ ├── validator.py # Validation des données
│ ├── normalizer.py # Normalisation et standardisation
│ ├── enricher.py # Enrichissement (calculs, ajouts)
│ └── type_converter.py # Conversion de types
├── loaders/ # Chargement (Load)
│ ├── init.py
│ ├── base_loader.py # Classe abstraite de base
│ ├── database_loader.py # Chargement vers base de données
│ ├── multi_table_loader.py # Chargement multi-tables
│ └── s3_loader.py # Charger brute dans le service
├── utils/ # Utilitaires
│ ├── init.py
│ ├── logger.py # Configuration des logs
│ ├── file_detector.py # Détection de format et encodage
│ ├── helpers.py # Fonctions utilitaires
│ ├── validators.py # Validateurs réutilisables
│ └── exceptions.py # Exceptions personnalisées
├── data/ # Données
│ ├── input/ # Fichiers sources
│ │ ├── csv/
│ │ ├── json/
│ │ ├── excel/
│ │ └── xml/
│ ├── output/ # Fichiers transformés
│ │ ├── csv/
│ │ ├── json/
│ │ └── excel/
│ ├── processed/ # Fichiers déjà traités (archive)
│ └── errors/ # Fichiers en erreur
├── logs/ # Suivi des exécutions
│ ├── etl_YYYYMMDD_HHMMSS.log
│ └── error.log
├── tests/ # Tests unitaires
│ ├── init.py
│ ├── test_extractors.py
│ ├── test_transformers.py
│ ├── test_loaders.py
│ ├── test_pipeline.py
│ └── fixtures/
│ ├── sample.csv
│ ├── sample.json
│ └── sample.xlsx
├── scripts/ # Scripts utilitaires
│ ├── setup_database.py # Initialisation de la BD
│ ├── migrate_data.py # Migration de données
│ └── generate_report.py # Génération de rapports
└── docs/ # Documentation
├── architecture.md
└── user_guide.md