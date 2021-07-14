class Base_converter:
    """This program converts whole and fractional numbers from any base between 2 and 36 to any base between 2 and 36"""

    def __init__(self):
        self.numeral = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" #instantiate and store numeral

    def to_base_ten(self, value, base):
        """
        This function converts bases between 2 and 36 to base 10. If 'numeral' is alphanumeric, then it'd be supplied as string.
        """
        numeral = self.numeral

        if not 2 <= base <= 36:
            raise ValueError('Base must be between 2 and 36')
        
        x = str(value)
        if '.' in x: #If value has a fractional part
            int_result = 0
            frac_result = 0
            int_part, frac_part = x.split('.')[0], x.split('.')[1] #split the value at '.' to two parts and return tuple
            int_result += int(int_part, base) #performing addition to confirm result is an integer
            
            for i in range(1, len(frac_part)+ 1): #exponent for fractional part starts from -1 to -n
                try:
                    frac_result += (int(frac_part[i-1]) / pow(base, i)) #If no alphabeth in fractional part
                except Exception:
                    frac_result += (int(numeral.index(frac_part[i-1])) / pow(base, i)) #Else look up value in numeral
            
            return int_result + frac_result #The retured value is an integer

        else: #If value is a whole number
            return int(str(value), base)


    def to_baseN(self, value, base, other_base = False):
        """
        This function converts bases between 2 and 36. 'value' is the amount to be converted and it's in decimal by default.
        The base to be converted to is supplied to 'base'. If value is not in base 10, its base should be supplied to 'other_base'.
        If 'value' is alphanumeric, it'd be supplied as string.
        """
        numeral = self.numeral
        
        if other_base: #If value is not in base 10
            conv_to_x = self.to_base_ten(value, other_base) #Use the above function to first convert to base 10.
            return self.to_baseN(conv_to_x, base) # Recursively convert from base 10 to the new base.

        else: # Since value supplied to this part is in decimal, we can work in base 10
            int_part = int(value) #Remove fractional part
            frac_part = value - int_part #Keep fractional part

            if value == 0:
                return "0"

            if int_part < 0:
                return '-' + self.to_baseN(abs(int_part), base, other_base) #for number < 0, work with its absolute form before adding -

            if not 2 <= base <= len(numeral):
                raise ValueError(f'Base must be between 2 and {len(numeral)}')
    
            int_result = "-" if int_part < 0 else "" #add - to negatiive numbers
            frac_result = ""

            while int_part != 0:
                int_result  += numeral[int_part % base]
                int_part //= base

            while frac_part != 0:
                frac_result += numeral[int(frac_part * base)]
                frac_part = (frac_part * base) - int(frac_part * base)
            result =  (int_result[::-1] + "." + frac_result[::1]) if frac_result else int_result[::-1]
           
            if result.startswith('.'):
                return "0" + result
            else:
                return result

if __name__ == "__main__":
    base_conv = Base_converter()
    print(base_conv.to_baseN(10.001, 16, other_base = 8))
    print(base_conv.to_baseN(-6, 2))
    print(base_conv.to_base_ten('23', 5))