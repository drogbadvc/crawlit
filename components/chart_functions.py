from collections import Counter
from components import DataPandas


class chart_functions:
    def http_status_code_by_depth_chart_apex(self, level, response_code_depth_200, df, response_code_depth_400,
                                             response_code_depth_500):
        options = {
            "chart": {
                "type": "bar",
                "stacked": True,
                "toolbar": {
                    "show": False
                }
            },
            "colors": ['#797bf2', '#ffeaa7', '#e17055', '#d63031'],
            "title": {
                "text": "HTTP Status Code by Depth Chart"
            },
            "xaxis": {
                "categories": level.tolist()
            },
            "yaxis": {
                "title": {
                    "text": "Number of Responses"
                }
            },
            "tooltip": {
                "shared": True,
                "intersect": False  # Ajoutez cette ligne
            },
            "legend": {
                "show": True,
                "position": "top"
            }
        }

        series = [
            {
                "name": "200",
                "data": response_code_depth_200['count'].tolist()
            },
            {
                "name": "300",
                "data": df['count'].tolist()
            },
            {
                "name": "400",
                "data": response_code_depth_400['count'].tolist()
            },
            {
                "name": "500",
                "data": response_code_depth_500['count'].tolist()
            }
        ]

        return options, series

    def pagerank_distribution_apex(self, dataframe):
        max_pagerank = dataframe['pagerank'].max()
        rounded_pageranks = [round(10 * (pr / max_pagerank)) for pr in dataframe['pagerank']]
        counted_pageranks = Counter(rounded_pageranks)

        options = {
            "chart": {
                "type": "bar",
                "toolbar": {
                    "show": False
                }
            },
            "title": {
                "text": "Pageranks distribution"
            },
            "xaxis": {
                "categories": list(range(10, 0, -1))  # Invert the order for x-axis
            },
            "yaxis": {
                "title": {
                    "text": "Number of URLs"
                }
            }
        }

        series = [{
            "name": "Number of URLs",
            "data": [counted_pageranks.get(i, 0) for i in range(10, 0, -1)]  # Invert the order for data
        }]

        return options, series

    def wordcount_distribution_apex(self, dataframe):
        # Segment the dataframe based on wordcount
        range_0_500 = dataframe[(dataframe['wordcount'] >= 0) & (dataframe['wordcount'] <= 500)].shape[0]
        range_500_1000 = dataframe[(dataframe['wordcount'] > 500) & (dataframe['wordcount'] <= 1000)].shape[0]
        range_1000_2000 = dataframe[(dataframe['wordcount'] > 1000) & (dataframe['wordcount'] <= 2000)].shape[0]
        range_2000_plus = dataframe[dataframe['wordcount'] > 2000].shape[0]

        categories = ["0-500", "500-1000", "1000-2000", "2000+"]
        values = [range_0_500, range_500_1000, range_1000_2000, range_2000_plus]

        options = {
            "chart": {
                "type": "bar",
                "toolbar": {
                    "show": False
                }
            },
            "title": {
                "text": "Word Count Distribution"
            },
            "xaxis": {
                "categories": categories
            },
            "yaxis": {
                "title": {
                    "text": "Number of Articles"
                }
            }
        }

        series = [{
            "name": "Number of Articles",
            "data": values
        }]

        return options, series

    def https_distribution_apex(self, dataframe):
        """Returns the distribution of URLs based on their protocol (HTTP or HTTPS)"""
        total_urls = dataframe.shape[0]
        https_count = dataframe[dataframe['url'].str.startswith('https://')].shape[0]
        http_count = dataframe[dataframe['url'].str.startswith('http://')].shape[0]

        https_percentage = (https_count / total_urls) * 100
        http_percentage = (http_count / total_urls) * 100

        options = {
            "chart": {
                "type": "radialBar",
                "toolbar": {
                    "show": False
                }
            },
            "colors": ['rgba(121, 123, 242, 0.85)', 'rgba(255, 34, 125, 0.85)'],  # Colors for HTTPS and HTTP
            "title": {
                "text": "Protocols Distribution"
            },
            "labels": ["HTTPS", "HTTP"],
            "legend": {
                "position": "top",
                "horizontalAlign": "center"
            },
            "dataLabels": {
                "enabled": True,
                "name": {
                    "show": True
                },
                "value": {
                    "show": True
                }
            },
            "responsive": [{
                "breakpoint": 480,
                "options": {
                    "chart": {
                        "width": 200
                    },
                    "legend": {
                        "position": "bottom"
                    }
                }
            }],
            "plotOptions": {
                "radialBar": {
                    "dataLabels": {
                        "total": {
                            "show": False,
                            "label": "Total",
                            "value": ""
                        }
                    }
                }
            }
        }

        series = [https_percentage, http_percentage]

        return options, series

    def status_code_apex(self, dataframe):
        response_200 = DataPandas().response_code(200, dataframe)
        response_301 = DataPandas().response_code(301, dataframe)
        response_404 = DataPandas().response_code(404, dataframe)
        response_500 = DataPandas().response_code(500, dataframe)

        options = {
            "chart": {
                "type": "pie",
                "toolbar": {
                    "show": False
                }
            },
            "colors": ['#8269b2', '#ffeaa7', '#e17055', '#d63031'],
            "title": {
                "text": "Response Code Distribution"
            },
            "labels": ["200", "301", "404", "500"],
            "legend": {
                "position": "top",
                "horizontalAlign": "center"
            },
            "dataLabels": {
                "enabled": False
            },
            "responsive": [{
                "breakpoint": 480,
                "options": {
                    "chart": {
                        "width": 200
                    },
                    "legend": {
                        "position": "bottom"
                    }
                }
            }]
        }

        series = [response_200, response_301, response_404, response_500]

        return options, series

    def meta_description_distribution_apex(self, dataframe):
        """Returns the percentage of pages with and without meta descriptions."""
        total_urls = dataframe.shape[0]
        with_meta_description = \
        dataframe[dataframe['meta_description'].notnull() & dataframe['meta_description'] != ''].shape[0]
        without_meta_description = total_urls - with_meta_description

        with_meta_description_percentage = (with_meta_description / total_urls) * 100
        without_meta_description_percentage = (without_meta_description / total_urls) * 100

        options = {
            "chart": {
                "type": "radialBar",
                "toolbar": {
                    "show": False
                }
            },
            "colors": ['rgba(121, 123, 242, 0.85)', 'rgba(255, 34, 125, 0.85)'],
            # Colors for With Meta Description and Without Meta Description
            "title": {
                "text": "Meta Description Distribution"
            },
            "labels": ["With Meta Description", "Without Meta Description"],
            "legend": {
                "position": "top",
                "horizontalAlign": "center"
            },
            "dataLabels": {
                "enabled": True,
                "name": {
                    "show": True
                },
                "value": {
                    "show": True
                }
            },
            "responsive": [{
                "breakpoint": 480,
                "options": {
                    "chart": {
                        "width": 200
                    },
                    "legend": {
                        "position": "bottom"
                    }
                }
            }],
            "plotOptions": {
                "radialBar": {
                    "dataLabels": {
                        "total": {
                            "show": False,
                            "label": "Total",
                            "value": ""
                        }
                    }
                }
            }
        }

        series = [with_meta_description_percentage, without_meta_description_percentage]

        return options, series

    def h1_distribution_apex(self, dataframe):
        """Returns the percentage of pages with and without H1 tags."""
        total_urls = dataframe.shape[0]
        with_h1 = dataframe[dataframe['h1'].notnull()].shape[0]
        without_h1 = total_urls - with_h1

        with_h1_percentage = (with_h1 / total_urls) * 100
        without_h1_percentage = (without_h1 / total_urls) * 100

        options = {
            "chart": {
                "type": "radialBar",
                "toolbar": {
                    "show": False
                }
            },
            "colors": ['rgba(121, 123, 242, 0.85)', 'rgba(255, 34, 125, 0.85)'],  # Colors for With H1 and Without H1
            "title": {
                "text": "H1 Distribution"
            },
            "labels": ["With H1", "Without H1"],
            "legend": {
                "position": "top",
                "horizontalAlign": "center"
            },
            "dataLabels": {
                "enabled": True,
                "name": {
                    "show": True
                },
                "value": {
                    "show": True
                }
            },
            "responsive": [{
                "breakpoint": 480,
                "options": {
                    "chart": {
                        "width": 200
                    },
                    "legend": {
                        "position": "bottom"
                    }
                }
            }],
            "plotOptions": {
                "radialBar": {
                    "dataLabels": {
                        "total": {
                            "show": False,
                            "label": "Total",
                            "value": ""
                        }
                    }
                }
            }
        }

        series = [with_h1_percentage, without_h1_percentage]

        return options, series

    def title_distribution_apex(self, dataframe):
        """Returns the percentage of pages with and without titles."""
        total_urls = dataframe.shape[0]
        with_title = dataframe[dataframe['title'].notnull()].shape[0]
        without_title = total_urls - with_title

        with_title_percentage = (with_title / total_urls) * 100
        without_title_percentage = (without_title / total_urls) * 100

        options = {
            "chart": {
                "type": "radialBar",
                "toolbar": {
                    "show": False
                }
            },
            "colors": ['rgba(121, 123, 242, 0.85)', 'rgba(255, 34, 125, 0.85)'],
            # Colors for With Title and Without Title
            "title": {
                "text": "Title Distribution"
            },
            "labels": ["With Title", "Without Title"],
            "legend": {
                "position": "top",
                "horizontalAlign": "center"
            },
            "dataLabels": {
                "enabled": True,
                "name": {
                    "show": True
                },
                "value": {
                    "show": True
                }
            },
            "responsive": [{
                "breakpoint": 480,
                "options": {
                    "chart": {
                        "width": 200
                    },
                    "legend": {
                        "position": "bottom"
                    }
                }
            }],
            "plotOptions": {
                "radialBar": {
                    "dataLabels": {
                        "total": {
                            "show": False,
                            "label": "Total",
                            "value": ""
                        }
                    }
                }
            }
        }

        series = [with_title_percentage, without_title_percentage]

        return options, series

