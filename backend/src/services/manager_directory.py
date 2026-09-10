"""
Manager Directory Service
Maintains the mapping of EMEA & Delivery Center Manager LDAPs to their full real names.
"""

MANAGER_CATALOG = {
    "ngada": "Ada Tagoe (Ng)",
    "akolkiewicz": "Agnieszka Kołkiewicz",
    "mcwilliam": "Alex McWilliam",
    "alveirogarcia": "Alveiro Garcia Niño",
    "amysouthwood": "Amy Southwood",
    "ageracitano": "Angelo Geracitano",
    "anibhaa": "Anibha Athalye",
    "ashishagar": "Ashish Agarwal",
    "carlogiovine": "Carlo Giovine",
    "dandreoli": "Dario Andreoli",
    "davidesteras": "David Esteras",
    "diegocat": "Diego Catania",
    "elikaplan": "Eli Kaplan",
    "fdiehl": "Falk Diehl",
    "fyn": "Fayan Pourisa",
    "finntoner": "Finn Toner",
    "flordi": "Floriana Di Pinto",
    "rinaudo": "Francesco Rinaudo",
    "gauravtaneja": "Gaurav Taneja",
    "halcohen": "Hal Cohen",
    "hugoalves": "Hugo Alves",
    "jtmartin": "Juan Turrión",
    "sincero": "Julio Sincero",
    "kzw": "Krzysztof Zawierucha",
    "kusaurabh": "Kumar Saurabh",
    "lcaggioni": "Lorenzo Caggioni",
    "ltomat": "Luca Tomat",
    "lfloretta": "Lucio Floretta",
    "msolares": "Mario Solares",
    "herterich": "Matthias M. Herterich",
    "ziener": "Matthias Ziener",
    "pawelglica": "Paweł Glica",
    "ptokarski": "Paweł Tokarski",
    "rmichalski": "Rafał Michalski",
    "ramanmadan": "Raman Madan",
    "nezharand": "Rand Nezha",
    "richardpaget": "Richard Paget",
    "seanhiggins": "Sean Higgins",
    "shruthireddyp": "Shruthi Reddy Paturi",
    "sofiadanko": "Sofia Danko",
    "sfreiberger": "Stefan Freiberger",
    "suchitpuri": "Suchit Puri",
    "sveneddicks": "Sven Eddicks",
    "vasugupta": "Vasu Gupta",
    "inigosoto": "Iñigo Soto",
    "tpanhard": "Thibault Panhard",
    "bastienp": "Bastien Prot",
    "apease": "Andrew Pease",
    "guptaashutosh": "Ashutosh Gupta",
    "thomascliett": "Thomas Cliett",
    "lynb": "Lyn Brady",
    "dorotheea": "Dorothee Andermann",
    "cuadrado": "Alberto Cuadrado",
    "francescobotta": "Francesco Botta",
    "sangwikar": "Sagar Sangwikar",
    "schuetzl": "Linus Schuetz",
    "rmizrahi": "Raphael Mizrahi",
    "emolvera": "Emmanuel Olvera"
}

def get_manager_name(ldap: str) -> str:
    """Returns the actual full name of a manager given their LDAP."""
    if not ldap:
        return ""
    clean = str(ldap).split("@")[0].strip().lower()
    return MANAGER_CATALOG.get(clean, clean.capitalize())
