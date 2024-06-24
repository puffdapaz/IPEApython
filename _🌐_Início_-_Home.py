import streamlit as st

def main():
    st.set_page_config(page_title = 'IPEA python'
                         , page_icon = '🐍'
                         , layout = 'wide'
                         , initial_sidebar_state = 'expanded')
    with st.sidebar:
        st.success('Selecione o idioma acima') 
        st.info('Select the langua above')
        st.markdown('[![GitHub](https://img.shields.io/badge/GitHub-181717.svg?style=for-the-badge&logo=GitHub&logoColor=white)](https://github.com/puffdapaz/pythonIPEA)')
        st.markdown('[![Article](https://img.shields.io/badge/Adobe%20Acrobat%20Reader-EC1C24.svg?style=for-the-badge&logo=Adobe-Acrobat-Reader&logoColor=white)](https://github.com/puffdapaz/pythonIPEA/blob/main/Impacto%20da%20receita%20tributária%20no%20desenvolvimento%20econômico%20e%20social.%20um%20estudo%20nos%20municípios%20brasileiros.pdf)')
        st.markdown('[![linkedIn](https://img.shields.io/badge/LinkedIn-0A66C2.svg?style=for-the-badge&logo=LinkedIn&logoColor=white)](https://www.linkedin.com/in/silvaph)')

    st.title("IPEA python")

    miro_iframe = """
    <iframe width = "768"
            height = "432"
            src = "https://miro.com/app/embed/uXjVOvi97iA=/?pres=1&frameId=3458764592909428823&embedId=794216144639&autoplay=yep"
            frameborder = "0"
            scrolling = "no"
            allow = "fullscreen
                   ; clipboard-read
                   ; clipboard-write" allowfullscreen>
    </iframe>
    """
    st.markdown(miro_iframe, unsafe_allow_html=True)

if __name__ == "__main__":
    main()