# -*- coding: utf-8 -*-
import re

oxide_pattern = re.compile(r"^"
                               r"("
                               r"Al2O3|"
                               r"BaO|"
                               r"CaO|"
                               r"CoO|"
                               r"CO2|"
                               r"Cr2O3|"
                               r"CuO|"
                               r"FeO|"
                               r"Fe2O3|"
                               r"TiO2|"
                               r"K2O|"
                               r"MgO|"
                               r"MnO|"
                               r"Mn2O3|"
                               r"Na2O|"
                               r"NiO|"
                               r"P2O5|"
                               r"SiO2|"
                               r"ZnO|"
                               r"ZrO2|"
                               r"F)"
                               r"$")

cation_pattern = re.compile(r"^"
                            r"("
                            r"Si|"
                            r"(IV.*|VI.*)?Al|"
                            r"Mg[12]?|"
                            r"Fe(2|3)?(\+)?( *tot)?|"
                            r"Ca|"
                            r"Ti|"
                            r"Mn|"
                            r"Na|"
                            r"Cr|"
                            r"Cu|"
                            r"Zn)"
                            r"$")
