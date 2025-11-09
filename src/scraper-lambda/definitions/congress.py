class Document:
    def __init__(self, api_client, data):
        self.api_client = api_client
        self.data = data

class Bill(Document):
    def __init__(self, api_client, data):
        super().__init__(api_client, data)
        self.congress = data["congress"]
        self.bill_type = data["type"].lower()
        self.bill_number = data["number"]
        self.bill_id = f"{data["type"]}{data["number"]}-{data["congress"]}"

    def get_id(self):
        return self.bill_id

    def get_title(self):
        return self.data.get("title")

    def get_latest_action_date(self):
        return self.data.get("latestAction", {}).get("actionDate", "")

    def get_published_date(self):
        return self.data.get("introducedDate", "")

    def get_text_count(self):
        return self.data.get("textVersions", {}).get("count", 0)

    def get_actions(self):
        if isinstance(self.data.get("actions"), dict) and "count" in self.data.get("actions", {}):
            self.data["actions"] = self.api_client.get_bill_actions(self.congress, self.bill_type, self.bill_number)

            actions = []
            for action in self.data["actions"]:
                new_action = {
                    "date": action.get("actionDate"),
                    "text": action.get("text"),
                    "code": action.get("actionCode")
                }
                actions.append(new_action)

            self.data["actions"] = actions

        else:
            self.data["actions"] = self.data.get("actions", [])
        return self.data["actions"]

    def get_latest_action(self):
        if not isinstance(self.data.get("actions"), list):
            actions = self.get_actions()
        else:
            actions = self.data.get("actions", [])
        
        if actions and len(actions) > 0:
            return actions[-1]
        else:
            return None
        
    def get_status(self):
        if not isinstance(self.data.get("actions"), list):
            actions = self.get_actions()
        else:
            actions = self.data.get("actions", [])
        
        if actions and len(actions) > 0:
            latest = actions[-1]
            code = latest.get("code")

            # filter by codes
            enacted = {36000, 37000, 38000, 39000, 40000}

            if code in enacted:
                self.data['status'] = "enacted"
            else:
                self.data['status'] = "pending"
        else:
            self.data['status'] = "pending"
        
        return self.data['status']

    def get_amendments(self):
        if isinstance(self.data.get("amendments"), dict) and "count" in self.data.get("amendments", {}):
            self.data["amendments"] = self.api_client.get_bill_amendments(self.congress, self.bill_type, self.bill_number)
        
            amendments = []
            for amendment in self.data["amendments"]:
                amendments.append(Amendment(self.api_client, amendment))

            self.data["amendments"] = amendments
            return self.data["amendments"]
        return []

    def get_committees(self):
        if isinstance(self.data.get("committees"), dict) and "count" in self.data.get("committees", {}):
            self.data["committees"] = self.api_client.get_bill_committees(self.congress, self.bill_type, self.bill_number)
        
            committees = []
            for committee in self.data["committees"]:
                new_committee = {
                    "name": committee.get("name"),
                    "code": committee.get("systemCode"),
                    "chamber": committee.get("chamber")
                }
                committees.append(new_committee)
            
            self.data["committees"] = committees
            return self.data["committees"]
        return []

    def get_subjects(self):
        subjects = []
        if isinstance(self.data.get("subjects"), dict) and "count" in self.data.get("subjects", {}):
            self.data["subjects"] = self.api_client.get_bill_subjects(self.congress, self.bill_type, self.bill_number)

            if len(self.data["subjects"]["legislativeSubjects"]) > 0:
                subjects = [subj.get("name", "") for subj in self.data["subjects"]["legislativeSubjects"]]
            if self.data["subjects"]["policyArea"]:
                subjects.append(self.data["subjects"]["policyArea"])
        
        self.data["subjects"] = subjects
        return self.data["subjects"]

    def get_summary(self):
        self.data["summary"] = ""
        if isinstance(self.data.get("summaries"), dict) and "count" in self.data.get("summaries", {}):
            self.data["summaries"] = self.api_client.get_bill_summaries(self.congress, self.bill_type, self.bill_number)
        
            if len(self.data["summaries"]) > 0:
                self.data["summary"] = self.data["summaries"][-1].get("text", "")
        
        return self.data["summary"]

    def get_text(self):
        if 'text' not in self.data:
            self.data["textVersions"] = self.api_client.get_bill_text(self.congress, self.bill_type, self.bill_number)

            if isinstance(self.data["textVersions"], list) and len(self.data["textVersions"]) > 0:
                recent = self.data["textVersions"][-1]

                text = ""
                for format in recent.get("formats", []):
                    url = format.get('url')
                    if format.get("type") == "Formatted Text" or format.get("type") == "PDF":
                        print(f"URL: {url}")

                        try:
                            text = self.api_client.get_document_text(url)
                            break  # Exit after successfully processing the first valid URL
                        except Exception as e:
                            print(f"Error fetching document text from {url}: {e}")

                self.data["text"] = text
            else:
                self.data["text"] = ""

        return self.data["text"]
    
    def get_sponsors(self):
        if 'sponsors' in self.data and 'people' not in self.data:
            sponsors = []
            for sponsor in self.data['sponsors']:
                new_sponsor = {
                    "name": sponsor.get("fullName"),
                    "state": sponsor.get("state"),
                    "party": sponsor.get("party"),
                    "district": sponsor.get("district"),
                    "bioguideId": sponsor.get("bioguideId"),
                }
                sponsors.append(new_sponsor)
            self.data['people'] = sponsors

        if 'people' in self.data:
            return self.data['people']
        return []

    def to_dict(self, text=False):
        """
        Return a dictionary object with only the essential attributes of the bill.
        Minimized to reduce API calls.
        """
        # one API call to get actions if not already present

        bill = {
            "title": self.data.get("title"),
            "text": "",  # Don't fetch text by default - too expensive
            "congress": self.congress,
            "bill_type": self.bill_type,
            "bill_number": self.bill_number,
            "bill_id": self.bill_id,
            "latest_action_date": self.get_latest_action_date() if self.get_latest_action_date() else self.data.get("introducedDate"),
            "published_date": self.data.get("introducedDate"),
            'actions': self.get_actions(),
            'people': self.get_sponsors(),
            'url': f'https://www.congress.gov/bill/{self.congress}/{self.bill_type}/{self.bill_number}',
            'status': self.get_status()
        }

        if text:
            bill['text'] = self.get_text()

        return bill


class Amendment(Document):
    def __init__(self, api_client, data):
        super().__init__(api_client, data)
        self.congress = data["congress"]
        self.amendment_type = data["type"]
        self.amendment_number = data["number"]

    def get_id(self):
        return self.data.get("amendmentId")

    def get_title(self):
        return self.data.get("title")

    def get_latest_action_date(self):
        return self.data.get("latestAction", {}).get("actionDate")

    def get_actions(self):
        endpoint = f"amendment/{self.congress}/{self.amendment_type}/{self.amendment_number}/actions"
        return self.api_client._make_request(endpoint).get("actions", [])

    def get_cosponsors(self):
        endpoint = f"amendment/{self.congress}/{self.amendment_type}/{self.amendment_number}/cosponsors"
        return self.api_client._make_request(endpoint).get("cosponsors", [])

    def get_amendments(self):
        endpoint = f"amendment/{self.congress}/{self.amendment_type}/{self.amendment_number}/amendments"
        return self.api_client._make_request(endpoint).get("amendments", [])

    def get_text_versions(self):
        endpoint = f"amendment/{self.congress}/{self.amendment_type}/{self.amendment_number}/text"
        return self.api_client._make_request(endpoint).get("textVersions", [])


class Law(Document):
    def __init__(self, api_client, data):
        super().__init__(api_client, data)
        self.congress = data["congress"]
        self.law_type = data["type"]
        self.law_number = data["number"]

    def get_id(self):
        return self.data.get("lawId")

    def get_title(self):
        return self.data.get("title")

    def get_latest_action_date(self):
        return self.data.get("enactedDate") # Laws have an enactedDate instead of latestAction

    def get_text_versions(self):
        endpoint = f"law/{self.congress}/{self.law_type}/{self.law_number}/text"
        return self.api_client._make_request(endpoint).get("textVersions", [])
