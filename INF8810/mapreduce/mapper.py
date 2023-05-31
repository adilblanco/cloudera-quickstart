#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import sys


# Valeur utilisée pour les enregistrements manquants
missing_record = "+9999"
# Codes acceptés pour la température et l'humidité
accepted_codes = "[01459]"

for line in sys.stdin:
    line = line.strip()

    # Extraction des champs nécessaires de chaque ligne
    (year, temperature, code_temperature, humidity, code_humidity) = (
        line[15:19],    # Année
        line[87:92],    # Température
        line[92:93],    # Code de qualité de la température
        line[93:98],    # Humidité
        # line[99:100],   # Code de qualité de l'humidité
        line[98:99],   # Code de qualité de l'humidité
    )

    # Vérification si les enregistrements ne sont pas manquants
    if (temperature != missing_record and humidity != missing_record):
        # Vérification des codes de qualité pour la température et l'humidité
        if (re.match(accepted_codes, code_temperature) and re.match(accepted_codes, code_humidity)):
            print ("%s\t%s\t%s" % (year, temperature, humidity))
