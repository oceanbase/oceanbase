let hidden = true;
let triangleDirection = true
function displayOrHidden() {
    let listDisplay = document.getElementById("ob_second_level");
    let triangle = document.getElementsByClassName("triangle")
    let triangleOpen =document.getElementsByClassName("triangleOpen")
    if (hidden) {
        listDisplay.style.display = "block";
        hidden = false;
    } else {
        listDisplay.style.display = "none";
        hidden = true;
    };
    if (triangleDirection) {
        for (let i = 0; i < triangle.length; i++) {
            triangle[i].className = 'triangleOpen'
        }
        triangleDirection = false;
    }else {
        for (let j = 0; j < triangleOpen.length; j++) {
            triangleOpen[j].className = 'triangle'
        }
        triangleDirection = true;
    };
    
}


