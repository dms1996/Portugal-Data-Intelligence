"""
Portugal Data Intelligence - Pillar-Specific Rule-Based Insights
=================================================================
Standalone functions that generate executive-level narrative commentary
for each individual macroeconomic pillar.

Extracted from InsightEngine to keep the facade class slim.
"""


def _safe(value, fmt: str = ".1f") -> str:
    """Format a numeric value safely, returning 'N/A' on failure."""
    if value is None:
        return "N/A"
    try:
        return f"{float(value):{fmt}}"
    except (TypeError, ValueError):
        return str(value)


# -- GDP -----------------------------------------------------------

def _insight_gdp(d: dict) -> dict:
    s = _safe
    growth = d.get("latest_growth")
    recent = d.get("recent_avg_growth")
    longrun = d.get("longrun_avg_growth")
    latest = d["latest_value"]
    peak_y = d["peak_year"]
    trough_y = d["trough_year"]
    change = d["overall_change_pct"]

    # Headline
    if growth is not None and growth > 3:
        headline = f"Robust economic expansion: Portugal's GDP grew {s(growth)}% in {d['latest_year']}"
    elif growth is not None and growth > 1:
        headline = f"Moderate growth sustained: GDP advanced {s(growth)}% in {d['latest_year']}"
    elif growth is not None and growth > 0:
        headline = f"Growth momentum fading: GDP expanded just {s(growth)}% in {d['latest_year']}"
    elif growth is not None:
        headline = f"Economic contraction: GDP declined {s(growth)}% in {d['latest_year']}"
    else:
        headline = f"GDP analysis covering {d['earliest_year']}-{d['latest_year']}"

    # Executive summary
    para1 = _gdp_para1(d, growth, recent, longrun)
    para2 = _gdp_para2(d)
    para3 = _gdp_para3(d, growth, recent, longrun)
    executive_summary = f"{para1}\n\n{para2}\n\n{para3}"

    # Key findings
    findings = [
        f"GDP {'expanded' if change > 0 else 'contracted'} by {s(abs(change))}% in cumulative terms from {d['earliest_year']} to {d['latest_year']}.",
        f"The long-run average annual growth rate stands at {s(longrun)}%, while the most recent three-year average is {s(recent)}%.",
        f"Peak GDP was recorded in {peak_y} ({s(d['peak_value'], '.0f')}), while the trough occurred in {trough_y} ({s(d['trough_value'], '.0f')}).",
    ]
    for ck, ci in d.get("crisis_impacts", {}).items():
        label = ci["label"]
        mg = ci.get("mean_growth")
        if mg is not None and mg < 0:
            impact_desc = "significant economic stress"
        elif mg is not None and longrun is not None and mg > longrun + 0.5:
            impact_desc = "resilience"
        elif mg is not None and longrun is not None and mg < longrun - 0.5:
            impact_desc = "significant stress"
        else:
            impact_desc = "performance broadly in line with the overall trend"
        findings.append(
            f"During the {label}, average GDP growth was {s(mg)}%, "
            f"indicating {impact_desc}."
        )
    findings = findings[:6]

    # Risk assessment
    risk = _assess_gdp_risk(d, growth, recent)

    # Recommendations
    recs = _gdp_recommendations(d, growth, recent)

    # Outlook
    outlook = _gdp_outlook(d, growth, recent, longrun)

    return {
        "pillar": "gdp",
        "headline": headline,
        "executive_summary": executive_summary,
        "key_findings": findings,
        "risk_assessment": risk,
        "recommendations": recs,
        "outlook": outlook,
    }


def _gdp_para1(d, growth, recent, longrun):
    s = _safe
    if growth is not None and growth > 3:
        tone = (
            f"Portugal's economy demonstrated robust expansion in {d['latest_year']}, "
            f"with GDP growing at {s(growth)}% year-on-year. This pace of growth exceeded "
            f"the long-run average of {s(longrun)}%, signalling strengthening economic momentum "
            f"and favourable underlying conditions."
        )
    elif growth is not None and growth > 1:
        tone = (
            f"The Portuguese economy maintained moderate growth in {d['latest_year']}, "
            f"posting a {s(growth)}% year-on-year expansion. While below the pace observed "
            f"during peak recovery periods, this rate is broadly consistent with Portugal's "
            f"structural growth potential and the long-run average of {s(longrun)}%."
        )
    elif growth is not None and growth > 0:
        tone = (
            f"Economic growth showed signs of deceleration in {d['latest_year']}, "
            f"with GDP advancing by just {s(growth)}% year-on-year. This represents a notable "
            f"slowdown relative to the long-run average of {s(longrun)}%, suggesting that "
            f"headwinds are beginning to weigh on economic activity."
        )
    elif growth is not None:
        tone = (
            f"Portugal entered a contractionary phase in {d['latest_year']}, "
            f"with GDP declining by {s(abs(growth))}% year-on-year. This reversal marks a "
            f"significant departure from the long-run average growth of {s(longrun)}% and "
            f"warrants close monitoring of underlying demand conditions."
        )
    else:
        tone = (
            f"The GDP dataset spans {d['earliest_year']} to {d['latest_year']}, covering "
            f"a period of significant macroeconomic evolution for Portugal."
        )
    return tone


def _gdp_para2(d):
    s = _safe
    longrun = d.get("longrun_avg_growth")
    paras = []
    for ck, ci in d.get("crisis_impacts", {}).items():
        label = ci["label"]
        mg = ci.get("mean_growth")
        if mg is not None and mg < 0:
            paras.append(
                f"The {label} period exerted considerable downward pressure on output, "
                f"with average GDP growth of {s(mg)}% per annum."
            )
        elif mg is not None:
            if longrun is not None and mg > longrun + 0.5:
                tone = "demonstrating notable economic resilience"
            elif longrun is not None and mg < longrun - 0.5:
                tone = "reflecting significant stress relative to the long-run trend"
            else:
                tone = "broadly in line with the overall trend"
            paras.append(
                f"During the {label}, Portugal maintained average GDP growth of {s(mg)}%, "
                f"{tone}."
            )
    if not paras:
        return (
            f"Over the full observation window, GDP moved from {s(d['earliest_value'], '.0f')} "
            f"to {s(d['latest_value'], '.0f')}, representing a cumulative change of "
            f"{s(d['overall_change_pct'])}%."
        )
    return " ".join(paras)


def _gdp_para3(d, growth, recent, longrun):
    s = _safe
    if recent is not None and longrun is not None:
        gap = recent - longrun
        if gap > 1:
            return (
                f"Recent momentum has been notably above trend, with the three-year average "
                f"growth rate of {s(recent)}% exceeding the long-run mean of {s(longrun)}% "
                f"by {s(gap)} percentage points. This above-trend expansion may reflect "
                f"post-crisis recovery dynamics and should be assessed for sustainability."
            )
        elif gap < -1:
            return (
                f"The recent three-year average growth of {s(recent)}% trails the historical "
                f"mean of {s(longrun)}% by {s(abs(gap))} percentage points, indicating "
                f"a loss of momentum. Structural reform implementation and investment "
                f"acceleration may be necessary to restore convergence toward potential output."
            )
        else:
            return (
                f"Recent growth of {s(recent)}% is broadly aligned with the long-run average "
                f"of {s(longrun)}%, suggesting the economy is operating near its potential "
                f"growth trajectory. Sustaining this pace will require continued structural "
                f"competitiveness and favourable external conditions."
            )
    return "Insufficient historical data to assess recent momentum against long-run trends."


def _assess_gdp_risk(d, growth, recent):
    s = _safe
    if growth is not None and growth < 0:
        return (
            f"HIGH RISK. The economy contracted by {s(abs(growth))}% in {d['latest_year']}. "
            f"Negative growth trajectories, if sustained, can trigger adverse feedback loops "
            f"through employment, fiscal revenues, and credit quality. Immediate policy "
            f"attention is warranted."
        )
    if recent is not None and recent < 1:
        return (
            f"ELEVATED RISK. Average growth over the past three years ({s(recent)}%) is below "
            f"the threshold needed to meaningfully reduce unemployment or stabilise public "
            f"finances. The economy is vulnerable to external shocks."
        )
    if recent is not None and recent > 3:
        return (
            f"LOW RISK with OVERHEATING WATCH. Strong recent growth ({s(recent)}%) may "
            f"generate inflationary pressures or asset price imbalances. Monitor capacity "
            f"utilisation and labour market tightness."
        )
    return (
        f"MODERATE RISK. Growth is positive but not sufficiently above trend to provide a "
        f"substantial buffer against downside scenarios. Vigilance on external demand "
        f"conditions and structural bottlenecks is recommended."
    )


def _gdp_recommendations(d, growth, recent):
    recs = []
    if growth is not None and growth < 1:
        recs.append(
            "Accelerate implementation of the Recovery and Resilience Plan (PRR) to "
            "boost public investment and crowd in private capital."
        )
        recs.append(
            "Consider targeted fiscal stimulus measures focused on productivity-enhancing "
            "sectors, including digital transformation and green transition."
        )
    if recent is not None and recent > 3:
        recs.append(
            "Monitor capacity constraints and labour shortages that could bottleneck "
            "growth and push inflation above the ECB target."
        )
    recs.append(
        "Strengthen export diversification to reduce dependence on tourism and "
        "European demand cycles."
    )
    recs.append(
        "Prioritise human capital development through vocational training alignment "
        "with high-growth sectors (technology, renewable energy, advanced manufacturing)."
    )
    return recs[:4]


def _gdp_outlook(d, growth, recent, longrun):
    s = _safe
    if recent is not None and recent > 2:
        return (
            f"The near-term outlook for Portuguese GDP is cautiously optimistic. With recent "
            f"growth averaging {s(recent)}%, the economy has demonstrated resilience. However, "
            f"convergence toward the EU average requires sustained structural reform and "
            f"investment in productivity. External risks, including global trade tensions and "
            f"energy price volatility, remain key factors to monitor."
        )
    if recent is not None and recent > 0:
        return (
            f"Portugal's GDP growth trajectory is expected to remain modest in the near term. "
            f"The recent average of {s(recent)}% suggests limited room for fiscal manoeuvre "
            f"without growth-enhancing reforms. Upside potential exists through EU-funded "
            f"investment programmes and continued tourism sector strength."
        )
    return (
        f"The economic outlook carries significant uncertainty. The recent contraction "
        f"underscores vulnerabilities in the growth model. Recovery will depend on the "
        f"effectiveness of counter-cyclical policy measures and the pace of external "
        f"demand normalisation."
    )


# -- Unemployment --------------------------------------------------

def _insight_unemployment(d: dict) -> dict:
    s = _safe
    latest = d["latest_value"]
    peak = d["peak_value"]
    peak_y = d["peak_year"]
    trough = d["trough_value"]
    trough_y = d["trough_year"]
    trend = d["trend"]
    change = d["overall_change_pct"]

    # Headline
    if latest < 7:
        headline = f"Labour market strength: unemployment at {s(latest)}% in {d['latest_year']}"
    elif latest < 10:
        headline = f"Moderate labour market conditions: unemployment at {s(latest)}% in {d['latest_year']}"
    elif latest < 14:
        headline = f"Elevated unemployment persists at {s(latest)}% in {d['latest_year']}"
    else:
        headline = f"Critical unemployment: rate at {s(latest)}% in {d['latest_year']}"

    # Executive summary
    para1 = (
        f"Portugal's labour market has undergone a significant transformation over the "
        f"analysis period ({d['earliest_year']}-{d['latest_year']}). The unemployment rate "
        f"{'declined' if trend == 'decreasing' else 'increased' if trend == 'increasing' else 'remained broadly stable'} "
        f"from {s(d['earliest_value'])}% to {s(latest)}%, representing a "
        f"{s(abs(change))} percentage point {'improvement' if change < 0 else 'deterioration'}."
    )

    overall_mean = d.get("mean")
    crisis_text = []
    for ck, ci in d.get("crisis_impacts", {}).items():
        label = ci["label"]
        mean_v = ci.get("mean_value")
        max_v = ci.get("max_value")
        if mean_v is not None and overall_mean is not None and mean_v > overall_mean + 1:
            impact_tone = "reflecting significant stress on the Portuguese labour market"
        elif mean_v is not None and overall_mean is not None and mean_v < overall_mean - 1:
            impact_tone = "demonstrating resilience in the labour market"
        else:
            impact_tone = "broadly in line with the overall trend"
        crisis_text.append(
            f"During the {label}, unemployment averaged {s(mean_v)}% and peaked at "
            f"{s(max_v)}%, {impact_tone}."
        )
    para2 = " ".join(crisis_text) if crisis_text else (
        f"The unemployment rate peaked at {s(peak)}% in {peak_y} before declining "
        f"to a trough of {s(trough)}% in {trough_y}."
    )

    if latest <= trough * 1.1:
        para3 = (
            f"The current rate of {s(latest)}% is near historical lows for the observation "
            f"period, indicating that the labour market recovery has been substantial. "
            f"However, structural issues, including skills mismatches and regional "
            f"disparities, continue to require policy attention."
        )
    else:
        para3 = (
            f"At {s(latest)}%, unemployment remains {s(latest - trough)} percentage points "
            f"above the period low of {s(trough)}% ({trough_y}). Further labour market "
            f"tightening will likely require continued economic expansion and targeted "
            f"active labour market policies."
        )

    executive_summary = f"{para1}\n\n{para2}\n\n{para3}"

    findings = [
        f"Unemployment {'fell' if change < 0 else 'rose'} by {s(abs(change))} percentage points over the full period.",
        f"Peak unemployment of {s(peak)}% was recorded in {peak_y}; the trough of {s(trough)}% occurred in {trough_y}.",
        f"The overall trend is classified as {trend}.",
    ]
    # Youth data
    for col_name, sec_data in d.get("secondary", {}).items():
        if "youth" in col_name.lower():
            findings.append(
                f"Youth unemployment averaged {s(sec_data['mean'])}%, with a latest reading "
                f"of {s(sec_data['latest'])}% - highlighting persistent generational disparity."
            )
            break
    for ck, ci in d.get("crisis_impacts", {}).items():
        findings.append(
            f"The {ci['label']} drove unemployment to an average of {s(ci.get('mean_value'))}%."
        )
    findings = findings[:6]

    # Risk
    if latest > 12:
        risk = (
            f"HIGH RISK. Unemployment at {s(latest)}% remains critically elevated, "
            f"creating significant social costs and constraining consumer demand. "
            f"Long-term unemployment hysteresis is a concern."
        )
    elif latest > 8:
        risk = (
            f"ELEVATED RISK. At {s(latest)}%, the labour market has not fully normalised. "
            f"Structural unemployment components may resist cyclical recovery, "
            f"requiring targeted intervention."
        )
    else:
        risk = (
            f"MODERATE RISK. Unemployment at {s(latest)}% indicates a healthy labour market, "
            f"though tightness may generate wage pressures. Monitor for skills gaps and "
            f"regional imbalances that could constrain further improvement."
        )

    recs = [
        "Expand vocational training and reskilling programmes aligned with digital and green economy demands.",
        "Strengthen active labour market policies, particularly for youth and long-term unemployed cohorts.",
        "Address regional disparities through incentives for investment in higher-unemployment interior regions.",
    ]
    if latest > 10:
        recs.append(
            "Consider temporary employment subsidies for sectors with highest job-creation potential."
        )
    else:
        recs.append(
            "Focus on quality of employment metrics, including contract types, wage growth, "
            "and productivity per worker, to ensure sustainable labour market outcomes."
        )

    if trend == "decreasing" and latest < 8:
        outlook = (
            f"The labour market outlook is positive. The downward trend in unemployment "
            f"is expected to continue, supported by economic growth and tourism sector "
            f"resilience. However, demographic pressures and emigration patterns may "
            f"tighten the labour supply, potentially constraining growth in labour-intensive sectors."
        )
    elif trend == "decreasing":
        outlook = (
            f"The declining unemployment trajectory is encouraging, though the pace of "
            f"improvement may slow as the economy approaches its natural rate. Policy focus "
            f"should shift from job creation volume to job quality and productivity enhancement."
        )
    else:
        outlook = (
            f"The labour market faces headwinds. Without sustained GDP growth above 2%, "
            f"material unemployment reduction will be difficult to achieve. Policy coordination "
            f"between education, industry, and employment services will be critical."
        )

    return {
        "pillar": "unemployment",
        "headline": headline,
        "executive_summary": executive_summary,
        "key_findings": findings,
        "risk_assessment": risk,
        "recommendations": recs,
        "outlook": outlook,
    }


# -- Credit --------------------------------------------------------

def _insight_credit(d: dict) -> dict:
    s = _safe
    latest = d["latest_value"]
    trend = d["trend"]
    change = d["overall_change_pct"]
    recent = d.get("recent_avg_growth")
    longrun = d.get("longrun_avg_growth")

    if trend == "decreasing":
        headline = f"Credit contraction: lending declined {s(abs(change))}% over the analysis period"
    elif recent is not None and recent > 3:
        headline = f"Credit expansion accelerates: recent growth averaging {s(recent)}% annually"
    else:
        headline = f"Credit conditions stabilise: latest outstanding balance at {s(latest, '.0f')} EUR million"

    para1 = (
        f"Credit to the Portuguese economy has exhibited a {trend} trajectory over the "
        f"{d['earliest_year']}-{d['latest_year']} period. Total outstanding credit moved "
        f"from {s(d['earliest_value'], '.0f')} to {s(latest, '.0f')} EUR million, a cumulative "
        f"change of {s(change)}%. This evolution reflects the interplay of deleveraging "
        f"pressures following the sovereign debt crisis, regulatory tightening, and the "
        f"subsequent normalisation of bank lending conditions."
    )

    crisis_parts = []
    for ck, ci in d.get("crisis_impacts", {}).items():
        mg = ci.get("mean_growth")
        if mg is not None and mg < 0:
            credit_tone = "with the banking sector under significant stress"
        elif mg is not None and longrun is not None and mg > longrun + 0.5:
            credit_tone = "showing resilience in lending capacity"
        elif mg is not None and longrun is not None and mg < longrun - 0.5:
            credit_tone = "reflecting significant stress in lending conditions"
        else:
            credit_tone = "broadly in line with the overall trend"
        crisis_parts.append(
            f"During the {ci['label']}, credit growth averaged {s(mg)}% annually, "
            f"{credit_tone}."
        )
    para2 = " ".join(crisis_parts) if crisis_parts else (
        f"The credit cycle in Portugal has been marked by a prolonged deleveraging phase "
        f"following the 2011-2014 financial stress, with gradual stabilisation in recent years."
    )

    if recent is not None and longrun is not None:
        if recent > longrun:
            para3 = (
                f"Recent credit dynamics ({s(recent)}% average growth) show improvement "
                f"relative to the long-run average ({s(longrun)}%), suggesting that "
                f"the deleveraging cycle may be approaching completion and that transmission "
                f"of monetary policy to credit conditions is improving."
            )
        else:
            para3 = (
                f"Despite accommodative monetary conditions, recent credit growth ({s(recent)}%) "
                f"remains below the long-run average ({s(longrun)}%), indicating persistent "
                f"structural headwinds in the banking sector's willingness or ability to lend."
            )
    else:
        para3 = "Credit market data suggests a gradual normalisation of lending conditions."

    executive_summary = f"{para1}\n\n{para2}\n\n{para3}"

    findings = [
        f"Total credit {'expanded' if change > 0 else 'contracted'} by {s(abs(change))}% over the full observation period.",
        f"Credit peaked at {s(d['peak_value'], '.0f')} in {d['peak_year']} and troughed at {s(d['trough_value'], '.0f')} in {d['trough_year']}.",
        f"Recent three-year average growth: {s(recent)}% vs long-run average: {s(longrun)}%.",
    ]
    # Secondary columns (NPL, segments)
    for col_name, sec_data in d.get("secondary", {}).items():
        cl = col_name.lower()
        if "npl" in cl or "non_performing" in cl:
            findings.append(
                f"Non-performing loan indicator ({col_name}) averaged {s(sec_data['mean'])}%, "
                f"with a latest reading of {s(sec_data['latest'])}%."
            )
        elif "household" in cl or "corporate" in cl or "nfc" in cl:
            findings.append(
                f"Segment '{col_name}' latest value: {s(sec_data['latest'], '.0f')}, "
                f"mean: {s(sec_data['mean'], '.0f')}."
            )
    findings = findings[:6]

    if trend == "decreasing" and (recent is None or recent < 0):
        risk = (
            f"HIGH RISK. Continued credit contraction signals impaired monetary policy "
            f"transmission and potential credit rationing. Small and medium enterprises "
            f"may face financing constraints that inhibit investment and growth."
        )
    elif trend == "decreasing":
        risk = (
            f"ELEVATED RISK. Although the pace of credit decline has moderated, the "
            f"overall contracting trend indicates lingering balance sheet repair needs "
            f"in the banking sector. Credit availability remains a potential bottleneck."
        )
    else:
        risk = (
            f"MODERATE RISK. Credit conditions appear to be normalising. The primary "
            f"risk lies in the quality of new lending and the adequacy of credit growth "
            f"to support the economy's investment needs without rebuilding excessive leverage."
        )

    recs = [
        "Monitor credit quality metrics closely, ensuring that credit expansion does not compromise underwriting standards.",
        "Support SME access to finance through guarantee programmes and development bank co-lending facilities.",
        "Encourage diversification of corporate financing toward capital markets and alternative lending platforms.",
        "Assess the effectiveness of ECB monetary policy transmission through Portuguese banking channels.",
    ]

    if trend == "increasing" and (recent is not None and recent > 3):
        outlook_text = (
            f"Credit conditions are expected to remain supportive, with lending growth "
            f"likely to moderate toward a sustainable pace as the ECB adjusts monetary policy. "
            f"The key risk is a potential tightening cycle that could cool credit expansion "
            f"prematurely if inflation concerns persist."
        )
    else:
        outlook_text = (
            f"The credit outlook remains cautious. While banking sector fundamentals have "
            f"improved significantly since the sovereign debt crisis, structural challenges "
            f"in Portugal's banking landscape, including consolidation pressures and digital "
            f"transformation costs, may constrain lending capacity. Regulatory developments "
            f"at the European level will be a key determinant."
        )

    return {
        "pillar": "credit",
        "headline": headline,
        "executive_summary": executive_summary,
        "key_findings": findings,
        "risk_assessment": risk,
        "recommendations": recs,
        "outlook": outlook_text,
    }


# -- Interest Rates ------------------------------------------------

def _insight_interest_rates(d: dict) -> dict:
    s = _safe
    latest = d["latest_value"]
    trend = d["trend"]
    change = d["overall_change_pct"]
    peak = d["peak_value"]
    peak_y = d["peak_year"]
    trough = d["trough_value"]
    trough_y = d["trough_year"]

    if latest < 1:
        headline = f"Ultra-low rate environment: primary rate at {s(latest)}% in {d['latest_year']}"
    elif latest < 3:
        headline = f"Rate normalisation underway: primary rate at {s(latest)}% in {d['latest_year']}"
    elif latest > 5:
        headline = f"Elevated rate environment: primary rate at {s(latest)}% in {d['latest_year']}"
    else:
        headline = f"Interest rates at {s(latest)}%: monetary conditions tightening in {d['latest_year']}"

    para1 = (
        f"The interest rate environment in Portugal has been shaped by extraordinary "
        f"monetary policy cycles over the {d['earliest_year']}-{d['latest_year']} period. "
        f"The primary rate measure moved from {s(d['earliest_value'])}% to {s(latest)}%, "
        f"reflecting the ECB's response to successive economic crises, a prolonged "
        f"zero-lower-bound period, and the subsequent normalisation of monetary policy."
    )

    # Sovereign spread commentary
    spread_text = ""
    for col_name, sec_data in d.get("secondary", {}).items():
        cl = col_name.lower()
        if "spread" in cl or "sovereign" in cl or "bond" in cl or "yield" in cl:
            spread_text = (
                f" Portuguese sovereign yields (proxy: {col_name}) averaged "
                f"{s(sec_data['mean'])}% over the period, with the latest reading at "
                f"{s(sec_data['latest'])}%. This spread dynamic is critical for assessing "
                f"the cost of government borrowing and financial stability."
            )
            break

    para2 = (
        f"Rates peaked at {s(peak)}% in {peak_y}, reflecting crisis-era monetary "
        f"conditions and sovereign risk premia, before declining to a trough of "
        f"{s(trough)}% in {trough_y} as the ECB deployed unprecedented accommodative "
        f"measures including negative deposit rates and quantitative easing.{spread_text}"
    )

    para3 = (
        f"The current rate of {s(latest)}% must be assessed in the context of the "
        f"ECB's inflation mandate and the broader euro area monetary policy stance. "
        f"For Portugal specifically, the transmission of rate changes to lending "
        f"conditions, mortgage costs, and sovereign debt servicing costs requires "
        f"careful monitoring given the economy's sensitivity to financing conditions."
    )

    executive_summary = f"{para1}\n\n{para2}\n\n{para3}"

    findings = [
        f"The primary interest rate moved from {s(d['earliest_value'])}% to {s(latest)}% over the observation period.",
        f"Peak rate of {s(peak)}% occurred in {peak_y}; trough of {s(trough)}% in {trough_y}.",
        f"Overall trend classified as {trend}.",
    ]
    for col_name, sec_data in d.get("secondary", {}).items():
        findings.append(
            f"{col_name}: mean {s(sec_data['mean'])}%, range [{s(sec_data['min'])}% - {s(sec_data['max'])}%], latest {s(sec_data['latest'])}%."
        )
    findings = findings[:6]

    if latest > 4:
        risk = (
            f"HIGH RISK. Elevated interest rates at {s(latest)}% pose significant "
            f"challenges for Portugal's debt-servicing capacity, mortgage holders, "
            f"and corporate investment. The risk of financial stress in rate-sensitive "
            f"sectors is material."
        )
    elif latest > 2:
        risk = (
            f"ELEVATED RISK. Rate normalisation to {s(latest)}% creates adjustment "
            f"pressures across the economy, particularly for borrowers who accumulated "
            f"debt during the low-rate period. Monitoring of household and corporate "
            f"debt-service ratios is essential."
        )
    elif latest < 0.5:
        risk = (
            f"MODERATE RISK (unusual conditions). Near-zero rates at {s(latest)}% support "
            f"borrowing costs but signal underlying economic weakness. Risks include "
            f"misallocation of capital, compressed bank margins, and future adjustment "
            f"costs when rates eventually normalise."
        )
    else:
        risk = (
            f"MODERATE RISK. The current rate of {s(latest)}% represents a transitional "
            f"monetary environment. Key risks include the pace and magnitude of future "
            f"rate adjustments and their differential impact across economic sectors."
        )

    recs = [
        "Conduct stress testing of public and private debt portfolios against further rate increases of 100-200 basis points.",
        "Encourage fixed-rate mortgage and lending products to reduce economy-wide exposure to rate volatility.",
        "Monitor the sovereign spread relative to euro area benchmarks as an early warning indicator for market confidence.",
        "Assess the impact of rate changes on bank profitability and lending capacity in the Portuguese banking sector.",
    ]

    if trend == "increasing":
        outlook = (
            f"The interest rate trajectory is expected to remain influenced by ECB "
            f"monetary policy decisions. With rates currently at {s(latest)}%, the outlook "
            f"depends heavily on euro area inflation dynamics. For Portugal, the critical "
            f"question is whether the economy can absorb higher financing costs without "
            f"triggering adverse feedback through the sovereign-bank-corporate nexus."
        )
    else:
        outlook = (
            f"The accommodative rate environment may persist if inflation remains subdued, "
            f"but policy normalisation represents a significant medium-term adjustment risk. "
            f"Portugal should use the current window to reduce debt levels, strengthen "
            f"fiscal buffers, and prepare institutional capacity for a higher-rate regime."
        )

    return {
        "pillar": "interest_rates",
        "headline": headline,
        "executive_summary": executive_summary,
        "key_findings": findings,
        "risk_assessment": risk,
        "recommendations": recs,
        "outlook": outlook,
    }


# -- Inflation -----------------------------------------------------

def _insight_inflation(d: dict) -> dict:
    s = _safe
    latest = d["latest_value"]
    trend = d["trend"]
    mean_val = d["mean"]
    peak = d["peak_value"]
    peak_y = d["peak_year"]
    trough = d["trough_value"]
    trough_y = d["trough_year"]

    if latest > 5:
        headline = f"Inflationary surge: headline rate at {s(latest)}% in {d['latest_year']}"
    elif latest > 3:
        headline = f"Above-target inflation: rate at {s(latest)}% exceeds ECB 2% objective in {d['latest_year']}"
    elif latest > 1.5:
        headline = f"Inflation near target: rate at {s(latest)}% consistent with price stability in {d['latest_year']}"
    elif latest > 0:
        headline = f"Low inflation environment: rate at {s(latest)}% in {d['latest_year']}"
    else:
        headline = f"Deflationary risk: inflation at {s(latest)}% in {d['latest_year']}"

    para1 = (
        f"Portugal's inflation dynamics over {d['earliest_year']}-{d['latest_year']} "
        f"reflect the broader European experience of successive price stability challenges. "
        f"Headline inflation averaged {s(mean_val)}% per annum, moving from {s(d['earliest_value'])}% "
        f"to {s(latest)}%. The overall trend is classified as {trend}, with significant "
        f"variation driven by external shocks, energy prices, and monetary policy transmission."
    )

    crisis_parts = []
    for ck, ci in d.get("crisis_impacts", {}).items():
        label = ci["label"]
        mean_v = ci.get("mean_value")
        if "energy" in ck.lower() or "covid" in ck.lower():
            crisis_parts.append(
                f"The {label} had a pronounced impact on prices, with inflation averaging "
                f"{s(mean_v)}% during the period, {'well above' if mean_v is not None and mean_v > 3 else 'reflecting the unusual'} "
                f"the ECB's 2% target."
            )
        else:
            crisis_parts.append(
                f"During the {label}, inflation averaged {s(mean_v)}%, "
                f"{'with disinflationary pressures dominating' if mean_v is not None and mean_v < 1 else 'reflecting cost-push factors'}."
            )
    para2 = " ".join(crisis_parts) if crisis_parts else (
        f"Inflation peaked at {s(peak)}% in {peak_y} and reached a low of {s(trough)}% in "
        f"{trough_y}, demonstrating the wide range of price dynamics experienced."
    )

    # Core vs headline
    core_text = ""
    for col_name, sec_data in d.get("secondary", {}).items():
        if "core" in col_name.lower():
            core_text = (
                f"Core inflation (excluding energy and food) averaged {s(sec_data['mean'])}%, "
                f"with a latest reading of {s(sec_data['latest'])}%. The gap between headline "
                f"and core measures provides insight into the persistence of price pressures."
            )
            break
    para3 = core_text or (
        f"The current inflation rate of {s(latest)}% must be assessed against the ECB's "
        f"2% target and in light of second-round effects from wage negotiations and "
        f"administered price adjustments in Portugal."
    )

    executive_summary = f"{para1}\n\n{para2}\n\n{para3}"

    findings = [
        f"Average inflation over the full period: {s(mean_val)}%.",
        f"Peak inflation of {s(peak)}% occurred in {peak_y}; minimum of {s(trough)}% in {trough_y}.",
        f"Inflation trend classified as {trend} across the observation window.",
    ]
    for col_name, sec_data in d.get("secondary", {}).items():
        cl = col_name.lower()
        if "core" in cl or "hicp" in cl or "cpi" in cl:
            findings.append(
                f"{col_name} averaged {s(sec_data['mean'])}%, latest: {s(sec_data['latest'])}%."
            )
        if "real_rate" in cl:
            findings.append(
                f"Estimated real interest rate averaged {s(sec_data['mean'])}%."
            )
    findings = findings[:6]

    if latest > 5:
        risk = (
            f"HIGH RISK. Inflation at {s(latest)}% is significantly above the ECB's 2% target, "
            f"eroding purchasing power and creating uncertainty for investment decisions. "
            f"Second-round effects through wage-price spirals are a material concern."
        )
    elif latest > 3:
        risk = (
            f"ELEVATED RISK. Above-target inflation at {s(latest)}% is squeezing real incomes "
            f"and may prompt further ECB tightening. Portugal's competitiveness could be "
            f"affected if domestic inflation persistently exceeds the euro area average."
        )
    elif latest < 0.5:
        risk = (
            f"ELEVATED RISK (deflation). With inflation at {s(latest)}%, the risk of "
            f"deflationary expectations becoming entrenched is non-trivial. Low inflation "
            f"also increases the real burden of debt, complicating fiscal consolidation."
        )
    else:
        risk = (
            f"LOW-TO-MODERATE RISK. Inflation at {s(latest)}% is broadly consistent with "
            f"price stability. The primary risk is an unexpected acceleration driven by "
            f"energy prices, supply chain disruptions, or domestic wage pressures."
        )

    recs = [
        "Monitor wage settlement patterns for signs of second-round effects that could entrench above-target inflation.",
        "Assess the distributional impact of inflation on lower-income households and consider targeted support measures.",
        "Evaluate the effectiveness of ECB monetary policy transmission to Portuguese consumer prices.",
        "Track core inflation divergence from the euro area average as an indicator of competitiveness dynamics.",
    ]

    if latest > 3:
        outlook = (
            f"Inflation is expected to moderate as energy price base effects diminish and "
            f"monetary policy tightening works through the economy. However, the pace of "
            f"disinflation is uncertain. For Portugal, services inflation and wage dynamics "
            f"in the tourism sector will be key determinants of the medium-term path."
        )
    elif latest < 1:
        outlook = (
            f"Low inflation may persist if demand remains subdued. The ECB's accommodative "
            f"stance aims to lift inflation expectations, but structural factors including "
            f"globalisation effects and demographic headwinds could keep price pressures "
            f"muted in Portugal and the broader euro area."
        )
    else:
        outlook = (
            f"The inflation outlook is balanced. Near-target inflation provides a stable "
            f"environment for economic planning and investment. The principal uncertainties "
            f"are external: energy market developments, global supply chains, and the "
            f"calibration of ECB monetary policy."
        )

    return {
        "pillar": "inflation",
        "headline": headline,
        "executive_summary": executive_summary,
        "key_findings": findings,
        "risk_assessment": risk,
        "recommendations": recs,
        "outlook": outlook,
    }


# -- Public Debt ---------------------------------------------------

def _insight_public_debt(d: dict) -> dict:
    s = _safe
    latest = d["latest_value"]
    trend = d["trend"]
    change = d["overall_change_pct"]
    peak = d["peak_value"]
    peak_y = d["peak_year"]
    trough = d["trough_value"]
    trough_y = d["trough_year"]
    primary_col = d.get("primary_col", "")

    is_ratio = any(kw in primary_col.lower() for kw in ["ratio", "gdp", "percent"])
    unit = "% of GDP" if is_ratio else "EUR million"

    if is_ratio:
        if latest > 120:
            headline = f"Debt sustainability concern: public debt at {s(latest)}% of GDP in {d['latest_year']}"
        elif latest > 100:
            headline = f"Elevated public debt: ratio at {s(latest)}% of GDP in {d['latest_year']}"
        elif latest > 60:
            headline = f"Debt above Maastricht threshold: {s(latest)}% of GDP in {d['latest_year']}"
        else:
            headline = f"Public debt within benchmark: {s(latest)}% of GDP in {d['latest_year']}"
    else:
        headline = f"Public debt at {s(latest, '.0f')} {unit} in {d['latest_year']}"

    para1 = (
        f"Portugal's public debt trajectory over {d['earliest_year']}-{d['latest_year']} "
        f"has been one of the defining challenges of the country's macroeconomic framework. "
        f"The primary measure ({d.get('primary_col', 'debt')}) moved from "
        f"{s(d['earliest_value'], '.1f' if is_ratio else '.0f')} to {s(latest, '.1f' if is_ratio else '.0f')} {unit}, "
        f"a cumulative change of {s(change)}%. This trajectory has been shaped by "
        f"the sovereign debt crisis, austerity programmes, and post-crisis recovery dynamics."
    )

    crisis_parts = []
    for ck, ci in d.get("crisis_impacts", {}).items():
        label = ci["label"]
        mean_v = ci.get("mean_value")
        max_v = ci.get("max_value")
        crisis_parts.append(
            f"During the {label}, debt {'averaged' if is_ratio else 'stood at an average of'} "
            f"{s(mean_v, '.1f' if is_ratio else '.0f')} {unit}, reaching "
            f"{s(max_v, '.1f' if is_ratio else '.0f')} at its peak."
        )
    para2 = " ".join(crisis_parts) if crisis_parts else (
        f"Debt peaked at {s(peak, '.1f' if is_ratio else '.0f')} {unit} in {peak_y}, "
        f"before {'declining' if trend == 'decreasing' else 'stabilising'} toward "
        f"the current level."
    )

    if trend == "decreasing":
        para3 = (
            f"The declining debt trend is a positive signal for fiscal sustainability. "
            f"However, at {s(latest, '.1f' if is_ratio else '.0f')} {unit}, Portugal remains "
            f"above the euro area average and the 60% Maastricht threshold (for debt-to-GDP). "
            f"Continued fiscal discipline and growth-enhancing reforms are essential to "
            f"maintain the downward trajectory and build fiscal buffers against future shocks."
        )
    elif trend == "increasing":
        para3 = (
            f"The rising debt trajectory raises sustainability concerns. At "
            f"{s(latest, '.1f' if is_ratio else '.0f')} {unit}, Portugal's fiscal space is "
            f"constrained, limiting the government's ability to respond to future economic "
            f"downturns. Credible medium-term fiscal consolidation plans are critical "
            f"to maintain market confidence and rating agency assessments."
        )
    else:
        para3 = (
            f"Debt stabilisation around {s(latest, '.1f' if is_ratio else '.0f')} {unit} "
            f"represents a transitional phase. The path forward depends on the balance "
            f"between primary surplus generation, nominal GDP growth, and the effective "
            f"interest rate on outstanding debt."
        )

    executive_summary = f"{para1}\n\n{para2}\n\n{para3}"

    findings = [
        f"Public debt {'increased' if change > 0 else 'decreased'} by {s(abs(change))}% over the full observation period.",
        f"Peak debt of {s(peak, '.1f' if is_ratio else '.0f')} {unit} recorded in {peak_y}; trough of {s(trough, '.1f' if is_ratio else '.0f')} in {trough_y}.",
        f"Overall debt trend classified as {trend}.",
    ]
    for col_name, sec_data in d.get("secondary", {}).items():
        cl = col_name.lower()
        if "balance" in cl or "deficit" in cl or "fiscal" in cl:
            findings.append(
                f"Fiscal balance ({col_name}): average {s(sec_data['mean'])}, latest {s(sec_data['latest'])}."
            )
        elif col_name != d.get("primary_col"):
            findings.append(
                f"{col_name}: mean {s(sec_data['mean'], '.1f')}, latest {s(sec_data['latest'], '.1f')}."
            )
    findings = findings[:6]

    if is_ratio and latest > 120:
        risk = (
            f"HIGH RISK. Debt-to-GDP at {s(latest)}% significantly exceeds the 60% Maastricht "
            f"threshold and the euro area average. Portugal remains vulnerable to interest "
            f"rate shocks, growth disappointments, and shifts in market sentiment. "
            f"Sovereign rating downgrades could trigger adverse feedback through bank "
            f"balance sheets."
        )
    elif is_ratio and latest > 90:
        risk = (
            f"ELEVATED RISK. At {s(latest)}% of GDP, public debt constrains fiscal policy "
            f"space and carries refinancing risk in a rising rate environment. The "
            f"debt-growth-interest rate dynamic must remain favourable to prevent "
            f"a self-reinforcing upward spiral."
        )
    elif is_ratio and latest > 60:
        risk = (
            f"MODERATE RISK. Debt at {s(latest)}% of GDP exceeds the Maastricht reference "
            f"but is on a manageable path if current fiscal discipline is maintained. "
            f"The primary risk is an external shock that reverses consolidation progress."
        )
    else:
        risk = (
            f"MODERATE RISK. Public debt levels are within manageable bounds. Continued "
            f"prudent fiscal management is necessary to maintain this position and build "
            f"counter-cyclical buffers."
        )

    recs = [
        "Maintain primary budget surpluses to ensure a declining debt trajectory, targeting a debt-to-GDP path below 100% within the medium-term fiscal framework.",
        "Extend the average maturity of public debt issuance to reduce rollover risk and lock in favourable financing conditions.",
        "Implement structural spending reviews to identify efficiency gains that support consolidation without compromising public investment.",
        "Develop contingency fiscal plans for adverse scenarios (growth shock, rate spike) to demonstrate institutional preparedness to markets and rating agencies.",
    ]

    if trend == "decreasing":
        outlook = (
            f"The fiscal outlook is cautiously positive. The declining debt trajectory, if "
            f"sustained, positions Portugal for a potential credit rating upgrade and "
            f"reduced sovereign risk premia. Key assumptions underpinning this outlook "
            f"are continued GDP growth above 1.5%, primary surpluses, and stable financing "
            f"conditions. EU fiscal governance reforms may impose additional consolidation "
            f"requirements."
        )
    else:
        outlook = (
            f"The fiscal outlook carries material risks. Without a credible medium-term "
            f"consolidation plan, Portugal's debt dynamics could deteriorate, particularly "
            f"if interest rates remain elevated. Demographic pressures on pension and "
            f"healthcare spending will intensify, requiring proactive reform to contain "
            f"age-related expenditure growth."
        )

    return {
        "pillar": "public_debt",
        "headline": headline,
        "executive_summary": executive_summary,
        "key_findings": findings,
        "risk_assessment": risk,
        "recommendations": recs,
        "outlook": outlook,
    }


# -- Generic fallback ----------------------------------------------

def _insight_generic(d: dict) -> dict:
    s = _safe
    pillar = d.get("pillar", "unknown")
    return {
        "pillar": pillar,
        "headline": f"{pillar.replace('_', ' ').title()}: latest value {s(d.get('latest_value'))} in {d.get('latest_year')}",
        "executive_summary": (
            f"Analysis of the {pillar.replace('_', ' ')} pillar covers "
            f"{d.get('earliest_year')}-{d.get('latest_year')}. The overall trend is "
            f"classified as {d.get('trend')}. The primary measure moved from "
            f"{s(d.get('earliest_value'))} to {s(d.get('latest_value'))}, representing "
            f"a cumulative change of {s(d.get('overall_change_pct'))}%."
        ),
        "key_findings": [
            f"Overall change: {s(d.get('overall_change_pct'))}%.",
            f"Peak: {s(d.get('peak_value'))} in {d.get('peak_year')}.",
            f"Trough: {s(d.get('trough_value'))} in {d.get('trough_year')}.",
        ],
        "risk_assessment": f"MODERATE RISK. Further analysis required for the {pillar.replace('_', ' ')} pillar.",
        "recommendations": [
            f"Conduct deeper analysis of {pillar.replace('_', ' ')} drivers and structural factors.",
        ],
        "outlook": f"The outlook for {pillar.replace('_', ' ')} depends on domestic and European-level policy developments.",
    }


# ---------------------------------------------------------------------------
# Dispatch dictionary mapping pillar names to their insight functions
# ---------------------------------------------------------------------------
PILLAR_DISPATCH = {
    "gdp": _insight_gdp,
    "unemployment": _insight_unemployment,
    "credit": _insight_credit,
    "interest_rates": _insight_interest_rates,
    "inflation": _insight_inflation,
    "public_debt": _insight_public_debt,
}
