#!/usr/bin/env python3
"""
Categorize all research topics into ~25 broad categories.
Uses keyword matching with priority ordering.
"""

import json
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "hindex.db"

# Categories with keywords (ordered by priority - more specific first)
CATEGORIES = {
    # === MEDICAL SPECIALTIES (specific first) ===
    "Oncology & Cancer": [
        "cancer", "tumor", "oncol", "leukemia", "lymphoma", "melanoma",
        "carcinoma", "myeloma", "sarcoma", "neoplasm", "metasta", "chemo",
        "radiother", "malignan"
    ],
    "Cardiovascular": [
        "cardio", "heart", "cardiac", "coronary", "artery", "arterial",
        "vascular", "atheroscl", "myocard", "aortic", "hypertens", "stroke",
        "thromb", "aneurysm", "arrhythm", "atrial", "ventricul", "infarct",
        "angioplas", "stent", "ischemic", "hemodynam"
    ],
    "Neuroscience & Neurology": [
        "neuro", "brain", "cognit", "alzheimer", "parkinson", "epilep",
        "cortex", "cerebr", "nervous system", "synap", "hippocam", "dementia",
        "multiple sclerosis", "spinal cord", "motor neuron", "neuropath",
        "amyotrophic", "als ", "huntington", "migraine", "headache",
        "circadian", "sleep", "melatonin"
    ],
    "Infectious Disease": [
        "infect", "virus", "viral", "bacteri", "hiv", "hepatitis", "covid",
        "pathogen", "malaria", "tuberculosis", "antibiotic", "antimicrob",
        "sepsis", "influenza", "herpes", "parasit", "fungal", "mycobact",
        "vaccine", "ebola", "dengue", "zika", "leptospir", "syphilis",
        "chlamydia", "gonorrhea", "measles", "polio"
    ],
    "Immunology": [
        "immun", "t-cell", "b-cell", "antibod", "cytokine", "inflamm",
        "autoimmun", "allerg", "lymphocyte", "macrophage", "interleukin",
        "toll-like", "complement", "antigen"
    ],
    "Genetics & Genomics": [
        "gene", "genom", "dna", "rna", "epigene", "crispr", "mutation",
        "chromosome", "heredit", "genetic", "sequenc", "transcript",
        "methylat", "polymorphism", "allele", "genotype", "phenotype"
    ],
    "Gastroenterology & Hepatology": [
        "gastro", "liver", "hepat", "intestin", "colon", "bowel", "gut ",
        "pancrea", "esophag", "stomach", "digest", "biliary", "gallbladder",
        "cirrhosis", "ibd", "crohn", "ulcer"
    ],
    "Pulmonary & Respiratory": [
        "lung", "pulmon", "respiratory", "airway", "asthma", "copd",
        "bronch", "alveol", "pneumon", "thorac", "ventilat"
    ],
    "Nephrology & Urology": [
        "kidney", "renal", "nephro", "urolog", "urin", "bladder", "prostat",
        "dialysis", "glomerul", "ureter"
    ],
    "Endocrinology & Metabolism": [
        "endocrin", "hormone", "diabet", "insulin", "thyroid", "adrenal",
        "pituitary", "metabol", "obesity", "glucos", "lipid", "cholesterol",
        "vitamin d", "vitamin b", "nutrition", "diet", "calori"
    ],
    "Ophthalmology": [
        "ophthalm", "eye ", "ocular", "retin", "cornea", "glauco", "cataract",
        "vision", "macular", "optic nerve"
    ],
    "Dermatology": [
        "dermat", "skin ", "cutaneous", "epiderm", "psoriasis", "eczema",
        "wound heal"
    ],
    "Orthopedics & Musculoskeletal": [
        "orthop", "bone ", "fractur", "joint", "arthrit", "osteopor",
        "musculoskel", "spine", "cartilage", "tendon", "ligament", "skeletal",
        "elbow", "knee", "hip ", "shoulder", "wrist", "ankle"
    ],
    "Obstetrics & Gynecology": [
        "obstet", "gynec", "pregnan", "fetal", "maternal", "placenta",
        "uterine", "ovarian", "endometri", "menstrua", "fertility", "ivf"
    ],
    "Pediatrics & Development": [
        "pediatr", "child", "infant", "neonat", "newborn", "adolesc",
        "developmental", "congenital", "birth defect"
    ],
    "Psychiatry & Mental Health": [
        "psych", "mental health", "depress", "anxiety", "schizo", "bipolar",
        "addiction", "substance abuse", "ptsd", "autism", "adhd", "suicid",
        "eating disorder", "anorexia", "bulimia", "body image", "dysmorphi"
    ],
    "Surgery & Surgical Specialties": [
        "surg", "transplant", "resection", "anastom", "laparoscop",
        "endoscop", "implant", "graft", "trauma", "emergenc"
    ],
    "Radiology & Imaging": [
        "imaging", "mri", "ct scan", "radiol", "ultrasound", "pet scan",
        "x-ray", "mammogr", "tomograph", "fluoroscop", "angiogra",
        "segmentation", "dosimetr"
    ],
    "Pharmacology & Drug Development": [
        "pharmaco", "drug ", "therapeutic", "medicin", "dosage", "toxicol",
        "pharmacokin", "clinical trial"
    ],
    "Public Health & Epidemiology": [
        "public health", "epidemiol", "population health", "health policy",
        "health services", "healthcare system", "global health", "health disparit",
        "preventive", "screening", "outbreak", "mortality", "morbidity",
        "meta-analysis", "systematic review", "biomarker"
    ],
    "Dentistry & Oral Health": [
        "dental", "tooth", "teeth", "oral ", "gingiv", "periodon",
        "endodont", "orthodont", "maxillofac", "mandib", "stoma"
    ],
    "ENT & Audiology": [
        "hearing", "audiol", "cochlea", "deaf", "otolar", "ear ",
        "throat", "laryn", "vocal", "speech", "tinnitus", "vestibul",
        "head and neck", "oropharyn"
    ],
    "Rheumatology": [
        "rheumat", "lupus", "connective tissue", "fibromyalg", "gout",
        "scleroderma", "vasculitis"
    ],
    "Hematology": [
        "hematol", "blood ", "anemia", "hemoglobin", "coagul", "platelet",
        "hemophilia", "thrombo"
    ],
    "Allergy & Asthma": [
        "allerg", "asthma", "anaphyla", "hypersensitiv"
    ],

    # === BASIC SCIENCES ===
    "Biochemistry & Molecular Biology": [
        "protein", "enzyme", "molecular", "biochem", "kinase", "receptor",
        "ligand", "pathway", "signaling", "cell cycle", "apoptosis",
        "mitochondri", "ribosom", "peptide", "collagen", "proteoglycan",
        "glycosaminoglycan", "phosphodiesterase"
    ],
    "Cell Biology": [
        "cell ", "cellular", "stem cell", "organelle", "membrane",
        "cytoskeleton", "nucleus", "vesicle"
    ],

    # === PHYSICAL SCIENCES & ENGINEERING ===
    "Physics & Astronomy": [
        "physic", "quantum", "particle", "hadron", "collid", "boson",
        "meson", "chromodynamic", "photon", "laser", "optic", "plasma",
        "condensed matter", "superconductor", "magnetic", "electr",
        "thermodynamic", "gravit", "cosmolog", "astrophys", "astrono",
        "dark matter", "galaxy", "stellar", "solar", "nuclear", "radioactiv",
        "radiation", "ion ", "neutron", "proton"
    ],
    "Chemistry": [
        "chemi", "catalys", "reaction", "synthesis", "compound", "polymer",
        "organic", "inorganic", "electrochemi", "spectroscop", "crystal"
    ],
    "Materials Science & Engineering": [
        "material", "nanotech", "nanowire", "nanoparticle", "alloy",
        "ceramic", "composite", "coating", "semiconductor", "biomaterial",
        "3d print", "additive manufactur", "metallurg", "corrosion",
        "concrete", "welding", "glass", "fiber"
    ],
    "Computer Science & AI": [
        "comput", "algorithm", "machine learning", "deep learning",
        "artificial intellig", " ai ", "neural network", "data mining",
        "software", "programming", "cybersecur", "cryptograph", "blockchain",
        "natural language", "computer vision", "robotics", "vlsi", "fpga",
        "network", "internet", "database", "cloud", "petri net"
    ],
    "Engineering": [
        "engineer", "circuit", "sensor", "signal process", "wireless",
        "antenna", "microelectron", "mems", "biomedical engineer", "device",
        "hvdc", "power system", "heat transfer", "boiling", "hydraulic",
        "propulsion", "rocket", "aerospace", "vehicle", "automotive",
        "mechanical", "fluid", "turbine", "combustion", "fuel cell",
        "energy harvest", "solar cell", "battery", "motor", "rotor",
        "vibration", "noise", "fatigue", "stress analysis", "brake",
        "welding", "machining", "manufactur"
    ],

    # === LIFE SCIENCES ===
    "Ecology & Environmental Science": [
        "ecolog", "ecosystem", "environment", "climate", "biodiversity",
        "conservation", "pollution", "sustainab", "carbon", "marine",
        "freshwater", "wildlife", "habitat", "biofuel", "biogas",
        "waste", "recycl", "water treatment", "air quality"
    ],
    "Plant Science & Agriculture": [
        "plant", "botan", "crop", "agricultur", "seed", "soil",
        "photosynthesis", "chlorophyll", "weed", "herbicide", "forestry",
        "pest", "insect", "fruit", "vegetable", "grain", "rice", "wheat",
        "soybean", "maize", "cotton", "ginger", "cucurbit"
    ],
    "Zoology & Animal Science": [
        "animal", "zoolog", "insect", "fish", "bird", "mammal", "reptile",
        "amphibian", "invertebrate", "beetle", "bee ", "ant ", "spider",
        "coleoptera", "hymenoptera", "entomolog", "veterinar", "livestock",
        "poultry", "aquaculture"
    ],
    "Microbiology": [
        "microb", "bacteri", "yeast", "biofilm", "probiotic", "ferment"
    ],
    "Paleontology & Geology": [
        "paleontol", "fossil", "geolog", "stratigraph", "seism", "earthquak",
        "volcanic", "tectonic", "sediment", "mineral", "petrol", "oil ",
        "gas ", "mining", "ore "
    ],

    # === SOCIAL SCIENCES & HUMANITIES ===
    "Economics & Business": [
        "econom", "financ", "market", "business", "trade", "investment",
        "banking", "monetary", "fiscal", "entrepreneur", "management",
        "accounting", "consumer", "franchise", "intellectual capital",
        "supply chain", "logistics"
    ],
    "Social Sciences": [
        "social", "sociolog", "anthropolog", "demograph", "migration",
        "ethnic", "gender", "inequality", "poverty", "urban", "rural",
        "community", "family", "crime", "justice", "law ", "legal",
        "policy", "governance", "politic", "census", "population",
        "employment", "welfare", "housing"
    ],
    "Education": [
        "education", "learning", "teaching", "curriculum", "student",
        "school", "university", "academic", "pedagog", "literacy"
    ],
    "Psychology": [
        "psychology", "behavior", "cognitive", "emotion", "personality",
        "memory", "attention", "perception", "motivation"
    ],
    "Humanities": [
        "histor", "philosophy", "literature", "linguist", "language",
        "culture", "religion", "art ", "music", "archaeolog", "ethics",
        "fashion", "textile", "media", "communication", "discourse"
    ],

    # === MATH & STATISTICS ===
    "Mathematics & Statistics": [
        "mathematic", "statistic", "algebra", "geometry", "calculus",
        "probability", "stochastic", "optimization", "regression",
        "bayesian", "topology", "differential equation", "graph theory",
        "game theory", "queuing", "combinatori", "number theory"
    ],
}

# Default category for unmatched
DEFAULT_CATEGORY = "Other/Interdisciplinary"


def categorize_topic(topic: str) -> str:
    """Categorize a single topic based on keywords."""
    topic_lower = topic.lower()

    for category, keywords in CATEGORIES.items():
        for keyword in keywords:
            if keyword in topic_lower:
                return category

    return DEFAULT_CATEGORY


def main():
    # Load all topics
    with open('/tmp/all_topics.json', 'r') as f:
        all_topics = json.load(f)

    print(f"Categorizing {len(all_topics)} topics...")

    # Categorize each topic
    categorized = {}
    category_counts = {}

    for topic in all_topics:
        category = categorize_topic(topic)
        categorized[topic] = category
        category_counts[category] = category_counts.get(category, 0) + 1

    # Print results
    print("\n=== CATEGORY DISTRIBUTION ===")
    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        pct = count / len(all_topics) * 100
        print(f"{cat:40s} {count:5d} ({pct:5.1f}%)")

    print(f"\n{'TOTAL':40s} {len(all_topics):5d}")

    # Save mapping
    with open('/tmp/topic_categories.json', 'w') as f:
        json.dump(categorized, f, indent=2)

    print(f"\nSaved mapping to /tmp/topic_categories.json")

    # Show some examples from each category
    print("\n=== SAMPLE FROM EACH CATEGORY ===")
    for cat in sorted(category_counts.keys()):
        examples = [t for t, c in categorized.items() if c == cat][:3]
        print(f"\n{cat}:")
        for ex in examples:
            print(f"  - {ex}")


if __name__ == "__main__":
    main()
