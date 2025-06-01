import pandas as pd

class AlphaGenerator:
    def __init__(self, data_source, tracker):
        self.data_source = data_source  # DataFrame with data fields
        self.tracker = tracker  # AlphaTracker instance
        self.templates = {}  # Store templates by category

    def add_template(self, name, template, category, operators, description=""):
        self.templates[name] = {
            "template": template,
            "category": category,
            "operators": operators,
            "description": description
        }

    def generate_alphas(self, template_name, data_fields, config=None):
        # Implementation for alpha generation from template
        pass

    def validate_data_fields(self, data_fields):
        """Validate if data fields exist in the source dataset."""
        valid_fields = []
        invalid_fields = []

        for field in data_fields:
            if isinstance(field, str) and '/' in field:
                # Handle ratio fields
                numerator, denominator = field.split('/')
                if len(self.data_source[self.data_source['id'] == numerator]) == 0:
                    invalid_fields.append((field, f"Invalid numerator: {numerator}"))
                elif len(self.data_source[self.data_source['id'] == denominator]) == 0:
                    invalid_fields.append((field, f"Invalid denominator: {denominator}"))
                else:
                    valid_fields.append(field)
            elif len(self.data_source[self.data_source['id'] == field]) == 0:
                invalid_fields.append((field, f"Invalid data field: {field}"))
            else:
                valid_fields.append(field)

        return valid_fields, invalid_fields

    def generate_alpha_batch(self, template_name, data_fields, base_config=None, output_file=None):
        """Generate a batch of alphas from template and data fields."""
        if template_name not in self.templates:
            raise ValueError(f"Template {template_name} not found")

        template = self.templates[template_name]["template"]
        operators = self.templates[template_name]["operators"]
        category = self.templates[template_name]["category"]

        valid_fields, invalid_fields = self.validate_data_fields(data_fields)

        new_alpha_df = pd.DataFrame(columns=self.tracker.df.columns)
        results = []

        # Process valid fields
        for i, field in enumerate(valid_fields):
            alpha_record = self._create_alpha_record(template, field, operators, category, base_config)
            results.append(alpha_record)

        # Process invalid fields with error status
        for i, (field, error_msg) in enumerate(invalid_fields):
            alpha_record = self._create_alpha_record(template, field, operators, category, base_config)
            alpha_record["status"] = "failed"
            alpha_record["note_1"] = error_msg
            results.append(alpha_record)

        # Create final dataframe
        df_final = pd.concat(results)

        # Add standard metadata
        df_final = self._add_metadata(df_final, base_config)

        if output_file:
            df_final.to_csv(output_file, index=False)

        return df_final