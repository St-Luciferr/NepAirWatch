"""US-EPA AQI from PM2.5 (2024 breakpoints) + health advisories (English / Nepali)."""

# (C_lo, C_hi, I_lo, I_hi)
PM25_BREAKPOINTS = [
    (0.0, 9.0, 0, 50),
    (9.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 125.4, 151, 200),
    (125.5, 225.4, 201, 300),
    (225.5, 325.4, 301, 500),
]

CATEGORIES = [
    (50, "Good", "#00e400", "Air quality is satisfactory.", "हावा स्वच्छ छ।"),
    (100, "Moderate", "#ffff00", "Unusually sensitive people should limit long outdoor exertion.",
     "संवेदनशील व्यक्तिले लामो समय बाहिर कडा परिश्रम नगर्नुहोस्।"),
    (150, "Unhealthy for Sensitive Groups", "#ff7e00",
     "Children, elderly and people with asthma/heart disease should reduce outdoor activity.",
     "बालबालिका, वृद्ध र दम/मुटुरोगीले बाहिरी गतिविधि घटाउनुहोस्।"),
    (200, "Unhealthy", "#ff0000", "Everyone should reduce outdoor exertion; wear an N95 mask.",
     "सबैले बाहिरी गतिविधि घटाउनुहोस्; N95 मास्क लगाउनुहोस्।"),
    (300, "Very Unhealthy", "#8f3f97", "Avoid outdoor activity; schools should consider closing outdoor events.",
     "बाहिर ननिस्कनुहोस्; विद्यालयले बाहिरी कार्यक्रम स्थगित गर्नुहोस्।"),
    (10_000, "Hazardous", "#7e0023", "Health emergency: stay indoors, use air purifiers.",
     "स्वास्थ्य आपतकाल: घरभित्रै बस्नुहोस्।"),
]


def pm25_to_aqi(c):
    if c is None:
        return None
    c = round(float(c), 1)
    for c_lo, c_hi, i_lo, i_hi in PM25_BREAKPOINTS:
        if c <= c_hi:
            return int(round((i_hi - i_lo) / (c_hi - c_lo) * (max(c, c_lo) - c_lo) + i_lo))
    return 500


def category(aqi):
    if aqi is None:
        return None
    for hi, name, color, adv_en, adv_ne in CATEGORIES:
        if aqi <= hi:
            return {"category": name, "color": color, "advice_en": adv_en, "advice_ne": adv_ne}
