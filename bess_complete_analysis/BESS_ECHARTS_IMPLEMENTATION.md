# BESS Analytics - ECharts Implementation Guide

## Overview
This document provides specific ECharts implementations for all BESS analytics visualizations, with complete configuration examples for each chart type.

## 1. Revenue Performance Bar Chart

```typescript
import * as echarts from 'echarts';

// Revenue Rankings Bar Chart
const revenueRankingOption: echarts.EChartsOption = {
  title: {
    text: 'ERCOT BESS Revenue Rankings - 2024',
    left: 'center',
    textStyle: {
      fontSize: 20,
      fontWeight: 'bold'
    }
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'shadow'
    },
    formatter: (params: any) => {
      const data = params[0];
      return `
        <div style="padding: 10px;">
          <strong>${data.name}</strong><br/>
          Total Revenue: $${(data.value / 1000000).toFixed(2)}M<br/>
          $/MW-yr: $${data.data.revenuePerMW.toLocaleString()}<br/>
          Energy: ${data.data.energyPct}% | AS: ${data.data.asPct}%
        </div>
      `;
    }
  },
  grid: {
    left: '3%',
    right: '4%',
    bottom: '15%',
    containLabel: true
  },
  xAxis: {
    type: 'category',
    data: bessNames,
    axisLabel: {
      rotate: 45,
      interval: 0,
      fontSize: 10
    }
  },
  yAxis: {
    type: 'value',
    name: 'Annual Revenue ($)',
    axisLabel: {
      formatter: (value: number) => `$${(value / 1000000).toFixed(1)}M`
    }
  },
  series: [{
    name: 'Revenue',
    type: 'bar',
    data: bessData.map(d => ({
      value: d.totalRevenue,
      data: d,
      itemStyle: {
        color: d.totalRevenue > median ? '#10b981' : '#ef4444'
      }
    })),
    label: {
      show: true,
      position: 'top',
      formatter: (params: any) => {
        const percentile = params.data.data.percentileRank;
        return percentile >= 90 ? 'Top 10%' : '';
      }
    }
  }]
};
```

## 2. Revenue Mix Evolution Stacked Area Chart

```typescript
// Revenue Mix Evolution Chart
const revenueMixOption: echarts.EChartsOption = {
  title: {
    text: 'BESS Revenue Mix Evolution (2022-2025)',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'cross',
      label: {
        backgroundColor: '#6a7985'
      }
    }
  },
  legend: {
    data: ['Energy Arbitrage', 'RegUp', 'RegDown', 'RRS', 'ECRS', 'NonSpin'],
    bottom: 0
  },
  grid: {
    left: '3%',
    right: '4%',
    bottom: '10%',
    containLabel: true
  },
  xAxis: {
    type: 'category',
    boundaryGap: false,
    data: ['2022 Q1', '2022 Q2', '2022 Q3', '2022 Q4', '2023 Q1', /* ... */]
  },
  yAxis: {
    type: 'value',
    max: 100,
    axisLabel: {
      formatter: '{value}%'
    }
  },
  series: [
    {
      name: 'Energy Arbitrage',
      type: 'line',
      stack: 'Total',
      smooth: true,
      areaStyle: {
        color: '#3b82f6'
      },
      emphasis: {
        focus: 'series'
      },
      data: energyPercentages
    },
    {
      name: 'RegUp',
      type: 'line',
      stack: 'Total',
      smooth: true,
      areaStyle: {
        color: '#10b981'
      },
      emphasis: {
        focus: 'series'
      },
      data: regUpPercentages
    },
    {
      name: 'RegDown',
      type: 'line',
      stack: 'Total',
      smooth: true,
      areaStyle: {
        color: '#f59e0b'
      },
      emphasis: {
        focus: 'series'
      },
      data: regDownPercentages
    },
    {
      name: 'RRS',
      type: 'line',
      stack: 'Total',
      smooth: true,
      areaStyle: {
        color: '#8b5cf6'
      },
      emphasis: {
        focus: 'series'
      },
      data: rrsPercentages
    },
    {
      name: 'ECRS',
      type: 'line',
      stack: 'Total',
      smooth: true,
      areaStyle: {
        color: '#ec4899'
      },
      emphasis: {
        focus: 'series'
      },
      data: ecrsPercentages
    },
    {
      name: 'NonSpin',
      type: 'line',
      stack: 'Total',
      smooth: true,
      areaStyle: {
        color: '#6366f1'
      },
      emphasis: {
        focus: 'series'
      },
      data: nonSpinPercentages
    }
  ]
};
```

## 3. Nodal Price Impact Scatter Plot

```typescript
// Nodal Price Impact Scatter
const nodalImpactOption: echarts.EChartsOption = {
  title: {
    text: 'Revenue vs Nodal Price Premium',
    left: 'center'
  },
  grid: {
    left: '3%',
    right: '7%',
    bottom: '7%',
    containLabel: true
  },
  tooltip: {
    formatter: (params: any) => {
      return `
        <strong>${params.data.name}</strong><br/>
        Nodal Premium: $${params.value[0]}/MWh<br/>
        Annual Revenue: $${(params.value[1] / 1000000).toFixed(2)}M<br/>
        Capacity: ${params.data.capacity} MW
      `;
    }
  },
  xAxis: {
    type: 'value',
    name: 'Avg Nodal Premium vs Hub ($/MWh)',
    nameLocation: 'middle',
    nameGap: 30,
    splitLine: {
      lineStyle: {
        type: 'dashed'
      }
    },
    axisLine: {
      onZero: true,
      lineStyle: {
        color: '#333'
      }
    }
  },
  yAxis: {
    type: 'value',
    name: 'Annual Revenue ($M)',
    nameLocation: 'middle',
    nameGap: 50,
    axisLabel: {
      formatter: (value: number) => `$${(value / 1000000).toFixed(1)}M`
    }
  },
  series: [{
    name: 'BESS Performance',
    type: 'scatter',
    symbolSize: (data: any) => Math.sqrt(data.capacity) * 5,
    data: bessData.map(d => ({
      name: d.resourceName,
      value: [d.nodalPremium, d.totalRevenue],
      capacity: d.capacityMW,
      itemStyle: {
        color: d.primaryStrategy === 'EnergyArbitrage' ? '#3b82f6' :
               d.primaryStrategy === 'ASFocused' ? '#10b981' :
               d.primaryStrategy === 'Hybrid' ? '#f59e0b' : '#6366f1'
      }
    })),
    emphasis: {
      focus: 'self',
      itemStyle: {
        shadowBlur: 10,
        shadowOffsetX: 0,
        shadowColor: 'rgba(0, 0, 0, 0.5)'
      }
    },
    markLine: {
      silent: true,
      lineStyle: {
        color: '#333',
        type: 'dashed'
      },
      data: [
        { xAxis: 0 },
        { yAxis: median }
      ],
      label: {
        formatter: (params: any) => {
          return params.lineIndex === 0 ? 'Hub Price' : 'Median Revenue';
        }
      }
    }
  }],
  visualMap: {
    min: 0,
    max: 200,
    dimension: 2,
    orient: 'vertical',
    right: 10,
    top: 'center',
    text: ['HIGH', 'LOW'],
    calculable: true,
    inRange: {
      symbolSize: [10, 70]
    }
  }
};
```

## 4. Bidding Strategy Clustering Visualization

```typescript
// Strategy Clustering Scatter Plot
const strategyClusterOption: echarts.EChartsOption = {
  title: {
    text: 'BESS Operating Strategy Clusters',
    subtext: '2024 Analysis',
    left: 'center'
  },
  grid: {
    left: '3%',
    right: '4%',
    bottom: '3%',
    containLabel: true
  },
  xAxis: {
    type: 'value',
    name: 'Daily Cycles',
    min: 0,
    max: 4,
    splitLine: {
      lineStyle: {
        type: 'dashed'
      }
    }
  },
  yAxis: {
    type: 'value',
    name: 'AS Revenue %',
    min: 0,
    max: 100,
    axisLabel: {
      formatter: '{value}%'
    }
  },
  series: [
    {
      name: 'AS Specialist',
      type: 'scatter',
      data: asSpecialistData,
      itemStyle: { color: '#10b981' },
      symbolSize: 20
    },
    {
      name: 'Hybrid Optimizer',
      type: 'scatter',
      data: hybridData,
      itemStyle: { color: '#f59e0b' },
      symbolSize: 20
    },
    {
      name: 'Arbitrage Master',
      type: 'scatter',
      data: arbitrageData,
      itemStyle: { color: '#3b82f6' },
      symbolSize: 20
    },
    {
      name: 'Conservative',
      type: 'scatter',
      data: conservativeData,
      itemStyle: { color: '#6b7280' },
      symbolSize: 20
    }
  ],
  legend: {
    orient: 'vertical',
    right: 10,
    top: 'center'
  }
};
```

## 5. State of Charge Heatmap

```typescript
// State of Charge Heatmap
const socHeatmapOption: echarts.EChartsOption = {
  title: {
    text: 'Typical BESS State of Charge Pattern',
    left: 'center'
  },
  tooltip: {
    position: 'top',
    formatter: (params: any) => {
      return `${params.name}<br/>SOC: ${params.value[2]}%`;
    }
  },
  grid: {
    height: '50%',
    top: '10%'
  },
  xAxis: {
    type: 'category',
    data: hours,
    splitArea: {
      show: true
    }
  },
  yAxis: {
    type: 'category',
    data: days,
    splitArea: {
      show: true
    }
  },
  visualMap: {
    min: 0,
    max: 100,
    calculable: true,
    orient: 'horizontal',
    left: 'center',
    bottom: '15%',
    inRange: {
      color: ['#ef4444', '#f59e0b', '#10b981'] // red to yellow to green
    }
  },
  series: [{
    name: 'State of Charge',
    type: 'heatmap',
    data: socData,
    label: {
      show: false
    },
    emphasis: {
      itemStyle: {
        shadowBlur: 10,
        shadowColor: 'rgba(0, 0, 0, 0.5)'
      }
    }
  }]
};
```

## 6. Revenue Attribution Waterfall Chart

```typescript
// Revenue Waterfall Chart
const waterfallOption: echarts.EChartsOption = {
  title: {
    text: 'Revenue Build-Up Analysis',
    subtext: 'EXAMPLE_BESS - 2024',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'shadow'
    },
    formatter: (params: any) => {
      const tar = params[1];
      return `${tar.name}<br/>${tar.seriesName}: $${(tar.value / 1000).toFixed(0)}k`;
    }
  },
  grid: {
    left: '3%',
    right: '4%',
    bottom: '3%',
    containLabel: true
  },
  xAxis: {
    type: 'category',
    data: ['Energy\nArbitrage', 'RegUp', 'RRS', 'ECRS', 'Location\nDiscount', 'Total']
  },
  yAxis: {
    type: 'value',
    axisLabel: {
      formatter: (value: number) => `$${value / 1000}k`
    }
  },
  series: [
    {
      name: 'Placeholder',
      type: 'bar',
      stack: 'Total',
      itemStyle: {
        borderColor: 'transparent',
        color: 'transparent'
      },
      emphasis: {
        itemStyle: {
          borderColor: 'transparent',
          color: 'transparent'
        }
      },
      data: [0, 95000, 137000, 175000, 195000, 0]
    },
    {
      name: 'Revenue',
      type: 'bar',
      stack: 'Total',
      label: {
        show: true,
        position: 'top',
        formatter: (params: any) => {
          return params.value >= 0 ? `+$${(params.value / 1000).toFixed(0)}k` : 
                                     `-$${(Math.abs(params.value) / 1000).toFixed(0)}k`;
        }
      },
      data: [
        { value: 95000, itemStyle: { color: '#3b82f6' } },
        { value: 42000, itemStyle: { color: '#10b981' } },
        { value: 38000, itemStyle: { color: '#8b5cf6' } },
        { value: 20000, itemStyle: { color: '#ec4899' } },
        { value: -8000, itemStyle: { color: '#ef4444' } },
        { value: 187000, itemStyle: { color: '#059669' } }
      ]
    }
  ]
};
```

## 7. Performance Radar Chart

```typescript
// Peer Comparison Radar Chart
const radarOption: echarts.EChartsOption = {
  title: {
    text: 'BESS Performance vs Peers',
    left: 'center'
  },
  legend: {
    data: ['Your BESS', 'Peer Average', 'Top Performer'],
    bottom: 0
  },
  radar: {
    indicator: [
      { name: 'Revenue/MW', max: 100 },
      { name: 'Cycles/Day', max: 100 },
      { name: 'AS Revenue', max: 100 },
      { name: 'Efficiency', max: 100 },
      { name: 'Location Premium', max: 100 }
    ],
    shape: 'polygon',
    splitNumber: 5,
    axisName: {
      color: 'rgb(100, 100, 100)'
    },
    splitLine: {
      lineStyle: {
        color: [
          'rgba(100, 100, 100, 0.1)',
          'rgba(100, 100, 100, 0.2)',
          'rgba(100, 100, 100, 0.3)',
          'rgba(100, 100, 100, 0.4)',
          'rgba(100, 100, 100, 0.5)'
        ].reverse()
      }
    },
    splitArea: {
      show: false
    },
    axisLine: {
      lineStyle: {
        color: 'rgba(100, 100, 100, 0.5)'
      }
    }
  },
  series: [{
    type: 'radar',
    data: [
      {
        value: [65, 70, 45, 80, 55],
        name: 'Your BESS',
        symbol: 'circle',
        symbolSize: 8,
        lineStyle: {
          color: '#3b82f6',
          width: 2
        },
        areaStyle: {
          color: 'rgba(59, 130, 246, 0.3)'
        }
      },
      {
        value: [50, 50, 50, 50, 50],
        name: 'Peer Average',
        lineStyle: {
          color: '#6b7280',
          width: 2,
          type: 'dashed'
        }
      },
      {
        value: [95, 85, 90, 92, 88],
        name: 'Top Performer',
        lineStyle: {
          color: '#10b981',
          width: 2,
          type: 'dotted'
        }
      }
    ]
  }]
};
```

## 8. Location Value Box Plot

```typescript
// Location Performance Box Plot
const boxPlotOption: echarts.EChartsOption = {
  title: {
    text: 'BESS Performance by Location Type',
    left: 'center'
  },
  tooltip: {
    trigger: 'item',
    axisPointer: {
      type: 'shadow'
    }
  },
  grid: {
    left: '10%',
    right: '10%',
    bottom: '15%'
  },
  xAxis: {
    type: 'category',
    data: ['Premium Nodes', 'Load Zones', 'Hubs'],
    boundaryGap: true,
    nameGap: 30,
    splitArea: {
      show: false
    },
    splitLine: {
      show: false
    }
  },
  yAxis: {
    type: 'value',
    name: 'Revenue ($/MW-yr)',
    splitArea: {
      show: true
    },
    axisLabel: {
      formatter: (value: number) => `$${value / 1000}k`
    }
  },
  series: [
    {
      name: 'Revenue Distribution',
      type: 'boxplot',
      data: [
        [140000, 165000, 185000, 195000, 220000],
        [80000, 105000, 125000, 145000, 170000],
        [60000, 75000, 85000, 95000, 110000]
      ],
      itemStyle: {
        color: '#3b82f6',
        borderColor: '#1e40af'
      }
    },
    {
      name: 'Outliers',
      type: 'scatter',
      data: [
        [0, 250000],
        [1, 195000],
        [2, 125000]
      ]
    }
  ]
};
```

## 9. Real-time Performance Gauge

```typescript
// Real-time Performance Gauge
const performanceGaugeOption: echarts.EChartsOption = {
  title: {
    text: 'Current Month Performance',
    left: 'center'
  },
  series: [
    {
      type: 'gauge',
      startAngle: 180,
      endAngle: 0,
      min: 0,
      max: 200,
      splitNumber: 10,
      center: ['50%', '65%'],
      radius: '80%',
      itemStyle: {
        color: '#3b82f6',
        shadowColor: 'rgba(59, 130, 246, 0.45)',
        shadowBlur: 10,
        shadowOffsetX: 2,
        shadowOffsetY: 2
      },
      progress: {
        show: true,
        roundCap: true,
        width: 18
      },
      pointer: {
        icon: 'path://M2090.36389,615.30999 L2090.36389,615.30999...',
        length: '75%',
        width: 16,
        offsetCenter: [0, '5%']
      },
      axisLine: {
        roundCap: true,
        lineStyle: {
          width: 18,
          color: [
            [0.25, '#ef4444'],
            [0.5, '#f59e0b'],
            [0.75, '#3b82f6'],
            [1, '#10b981']
          ]
        }
      },
      axisTick: {
        splitNumber: 2,
        lineStyle: {
          width: 2,
          color: '#999'
        }
      },
      splitLine: {
        length: 12,
        lineStyle: {
          width: 3,
          color: '#999'
        }
      },
      axisLabel: {
        distance: 25,
        color: '#999',
        fontSize: 12,
        formatter: '{value}%'
      },
      title: {
        show: false
      },
      detail: {
        backgroundColor: '#fff',
        borderColor: '#999',
        borderWidth: 2,
        width: '80%',
        lineHeight: 40,
        height: 40,
        borderRadius: 8,
        offsetCenter: [0, '35%'],
        valueAnimation: true,
        formatter: (value: number) => {
          return `${value}% of Target\n$${(value * 1500).toLocaleString()}/MW`;
        },
        rich: {
          value: {
            fontSize: 20,
            fontWeight: 'bolder',
            color: '#777'
          },
          unit: {
            fontSize: 16,
            color: '#999'
          }
        }
      },
      data: [
        {
          value: 125,
          name: 'Performance vs Target'
        }
      ]
    }
  ]
};
```

## 10. Time Series with Events

```typescript
// Monthly Performance with Events
const timeSeriesOption: echarts.EChartsOption = {
  title: {
    text: 'BESS Monthly Revenue Trend',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'cross'
    }
  },
  toolbox: {
    feature: {
      dataView: { show: true, readOnly: false },
      restore: { show: true },
      saveAsImage: { show: true }
    }
  },
  legend: {
    data: ['Your BESS', 'Market Average', 'Top Quartile'],
    bottom: 0
  },
  grid: {
    left: '3%',
    right: '4%',
    bottom: '10%',
    containLabel: true
  },
  xAxis: {
    type: 'category',
    boundaryGap: false,
    data: months
  },
  yAxis: {
    type: 'value',
    name: '$/MW',
    axisLabel: {
      formatter: (value: number) => `$${value / 1000}k`
    }
  },
  series: [
    {
      name: 'Your BESS',
      type: 'line',
      data: yourBessData,
      smooth: true,
      symbol: 'circle',
      symbolSize: 8,
      lineStyle: {
        width: 3,
        color: '#3b82f6'
      },
      itemStyle: {
        color: '#3b82f6'
      },
      markPoint: {
        data: [
          { type: 'max', name: 'Max' },
          { type: 'min', name: 'Min' }
        ]
      },
      markLine: {
        silent: true,
        data: [
          {
            name: 'Storm Uri',
            xAxis: 'Feb 2021',
            label: {
              position: 'end',
              formatter: 'Storm Uri'
            }
          },
          {
            name: 'ECRS Launch',
            xAxis: 'Jun 2023',
            label: {
              position: 'end',
              formatter: 'ECRS Launch'
            }
          }
        ]
      }
    },
    {
      name: 'Market Average',
      type: 'line',
      data: marketAvgData,
      smooth: true,
      lineStyle: {
        width: 2,
        type: 'dashed',
        color: '#6b7280'
      }
    },
    {
      name: 'Top Quartile',
      type: 'line',
      data: topQuartileData,
      smooth: true,
      lineStyle: {
        width: 2,
        type: 'dotted',
        color: '#10b981'
      }
    }
  ]
};
```

## Implementation Tips

### 1. Responsive Design
```typescript
// Make charts responsive
window.addEventListener('resize', () => {
  myChart.resize();
});

// Or use ResizeObserver
const resizeObserver = new ResizeObserver(() => {
  myChart.resize();
});
resizeObserver.observe(chartContainer);
```

### 2. Theme Configuration
```typescript
// Dark theme option
const darkTheme = {
  backgroundColor: '#1f2937',
  textStyle: {
    color: '#e5e7eb'
  },
  // ... other theme settings
};

// Apply theme
const chart = echarts.init(dom, 'dark');
```

### 3. Export Functionality
```typescript
// Export chart as image
const exportChart = () => {
  const url = myChart.getDataURL({
    pixelRatio: 2,
    backgroundColor: '#fff'
  });
  
  const a = document.createElement('a');
  a.download = 'bess-performance.png';
  a.href = url;
  a.click();
};
```

### 4. Real-time Updates
```typescript
// Update chart with new data
const updateChart = (newData: any) => {
  myChart.setOption({
    series: [{
      data: newData
    }]
  });
};

// Set up WebSocket or polling
setInterval(() => {
  fetchLatestData().then(updateChart);
}, 60000); // Update every minute
```

## Next.js Integration

```tsx
// components/charts/RevenueChart.tsx
import { useEffect, useRef } from 'react';
import * as echarts from 'echarts';
import type { EChartsOption } from 'echarts';

export function RevenueChart({ data }: { data: any }) {
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstance = useRef<echarts.ECharts>();

  useEffect(() => {
    if (!chartRef.current) return;

    // Initialize chart
    chartInstance.current = echarts.init(chartRef.current);
    
    // Set option
    const option: EChartsOption = {
      // ... chart configuration
    };
    
    chartInstance.current.setOption(option);

    // Cleanup
    return () => {
      chartInstance.current?.dispose();
    };
  }, [data]);

  return <div ref={chartRef} className="w-full h-[400px]" />;
}
```