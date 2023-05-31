#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys


# Fonction pour calculer la moyenne
def calculate_mean(data):
    return sum(data) / len(data)


# Fonction pour calculer la covariance
def calculate_covariance(data1, data2):
    mean1 = calculate_mean(data1)
    mean2 = calculate_mean(data2)
    covariance = sum((x - mean1) * (y - mean2) for x, y in zip(data1, data2)) / len(data1)
    return covariance


# Fonction pour calculer la corrélation
def calculate_correlation(data1, data2):
    covariance = calculate_covariance(data1, data2)
    standard_deviation1 = calculate_standard_deviation(data1)
    standard_deviation2 = calculate_standard_deviation(data2)
    correlation = covariance / (standard_deviation1 * standard_deviation2)
    return correlation


# Fonction pour calculer l'écart-type
def calculate_standard_deviation(data):
    mean = calculate_mean(data)
    variance = sum((x - mean) ** 2 for x in data) / len(data)
    standard_deviation = variance**0.5
    return standard_deviation


# Dictionnaire pour stocker les températures et humidités par année
temperature_humidity_correlation = {}

for line in sys.stdin:
    line = line.strip()
    year, temperature, humidity = line.split("\t")

    if year not in temperature_humidity_correlation:
        temperature_humidity_correlation[year] = []

    temperature_humidity_correlation[year].append((int(temperature), int(humidity)))

# Calcul de la corrélation pour chaque année
for year, data in temperature_humidity_correlation.items():
    temperatures = [item[0] for item in data]
    humidities = [item[1] for item in data]

    correlation = calculate_correlation(temperatures, humidities)

    print ("%s\t%s" % (year, correlation))
