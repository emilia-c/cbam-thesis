# inspiration from https://github.com/felixrech/hys_scraper (their code)
from hys_scraper import HYS_Scraper

# Feedback on CBAM overall
# url: https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/12228-EU-Green-Deal-carbon-border-adjustment-mechanism-/feedback_en?p_id=7587254 
feedbacks, countries, categories = HYS_Scraper("7587254").scrape()

# Feedback on CBAM Declarants
# url: https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/14116-Carbon-border-adjustment-mechanism-CBAM-authorising-CBAM-declarants/feedback_en?p_id=33194896
feedbacks, countries, categories = HYS_Scraper("33194896").scrape()

# Feedback on CBAM Registry
# url: https://ec.europa.eu/info/law/better-regulation/have-your-say/initiatives/14117-Carbon-border-adjustment-mechanism-CBAM-establishment-of-CBAM-Registry/feedback_en?p_id=33195975
feedbacks, countries, categories = HYS_Scraper("33195975").scrape()